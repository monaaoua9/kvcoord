package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/monaaoua9/kvcoord/client"
)

func main() {
	var (
		eps   = flag.String("endpoints", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma separated host:port")
		conc  = flag.Int("c", 32, "concurrent clients")
		dur   = flag.Duration("d", 10*time.Second, "how long to run")
		size  = flag.Int("size", 128, "value size in bytes")
		keys  = flag.Int("keys", 10000, "key space")
		mix   = flag.String("mix", "put", "put | get | mixed (50/50) | get-serializable")
		token = flag.String("token", os.Getenv("KVCTL_TOKEN"), "bearer token")
		ready = flag.Duration("wait", 30*time.Second, "how long to wait for the cluster")
	)
	flag.Parse()

	cl, err := client.New(client.Config{Endpoints: strings.Split(*eps, ","), RequestTimeout: 5 * time.Second, Token: *token})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()

	wctx, cancel := context.WithTimeout(context.Background(), *ready)
	var probeErr error
	for {
		if _, probeErr = cl.Put(wctx, "bench/probe", "x"); probeErr == nil {
			break
		}
		if wctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "cluster not ready:", probeErr)
			os.Exit(1)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()

	val := strings.Repeat("v", *size)
	if *mix != "put" {
		// something to read
		for i := 0; i < *keys; i++ {
			if _, err := cl.Put(context.Background(), fmt.Sprintf("bench/%06d", i), val); err != nil {
				fmt.Fprintln(os.Stderr, "preload:", err)
				os.Exit(1)
			}
		}
	}

	ctx, stop := context.WithTimeout(context.Background(), *dur)
	defer stop()
	var (
		wg      sync.WaitGroup
		errs    atomic.Int64
		mu      sync.Mutex
		lats    []time.Duration
		counter atomic.Uint64
		started = time.Now()
	)
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for ctx.Err() == nil {
				n := counter.Add(1)
				key := fmt.Sprintf("bench/%06d", n%uint64(*keys))
				t0 := time.Now()
				var err error
				switch {
				case *mix == "put", *mix == "mixed" && n%2 == 0:
					_, err = cl.Put(ctx, key, val)
				case *mix == "get-serializable":
					_, err = cl.Get(ctx, key, client.WithSerializable())
				default:
					_, err = cl.Get(ctx, key)
				}
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				mine = append(mine, time.Since(t0))
			}
			mu.Lock()
			lats = append(lats, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	el := time.Since(started)

	slices.Sort(lats)
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[min(len(lats)-1, int(float64(len(lats))*p))]
	}
	fmt.Printf("mix=%s clients=%d size=%dB\n", *mix, *conc, *size)
	fmt.Printf("ops=%d errors=%d throughput=%.0f ops/s\n", len(lats), errs.Load(), float64(len(lats))/el.Seconds())
	fmt.Printf("latency p50=%v p90=%v p99=%v max=%v\n", pct(.5), pct(.9), pct(.99), pct(1))
}
