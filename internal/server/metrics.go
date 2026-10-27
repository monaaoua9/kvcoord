package server

import (
	"fmt"
	"net/http"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

// HTTPHandler serves /healthz, /readyz and a Prometheus text /metrics.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	// ready means: part of a cluster that currently has a leader
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.node.Status().Leader == 0 {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	mux.HandleFunc("/metrics", s.writeMetrics)
	return mux
}

func (s *Server) writeMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	st := s.node.Status()
	fs := s.fsm.Stats()

	emit := func(name, typ, help string, v any) {
		fmt.Fprintf(w, "# HELP kvcoord_%s %s\n# TYPE kvcoord_%s %s\nkvcoord_%s %v\n", name, help, name, typ, name, v)
	}
	isLeader := 0
	if st.State == raft.Leader {
		isLeader = 1
	}
	emit("raft_term", "gauge", "current raft term", st.Term)
	emit("raft_is_leader", "gauge", "1 if this node is the leader", isLeader)
	emit("raft_commit_index", "gauge", "highest committed log index", st.Commit)
	emit("raft_applied_index", "gauge", "highest applied log index", st.Applied)
	emit("raft_last_index", "gauge", "last log index", st.LastIndex)
	emit("raft_snapshot_index", "gauge", "log index of the latest in-memory compaction point", st.SnapIndex)
	emit("raft_dropped_messages_total", "counter", "inbound raft messages dropped because the queue was full", s.node.Dropped())
	emit("proposals_total", "counter", "write proposals submitted", s.m.proposals.Load())
	emit("proposal_errors_total", "counter", "write proposals that failed", s.m.proposalErrs.Load())
	emit("linearizable_reads_total", "counter", "linearizable reads served", s.m.reads.Load())
	emit("watch_events_total", "counter", "events pushed to watchers", s.m.watchEvents.Load())
	emit("lock_waits_total", "counter", "lock requests that had to queue", s.m.lockWaits.Load())
	emit("lease_expiries_total", "counter", "leases expired by this node while leader", s.m.leaseExpiries.Load())
	emit("keys", "gauge", "number of keys", fs.Keys)
	emit("revision", "gauge", "current store revision", fs.Revision)
	emit("leases", "gauge", "live leases", fs.Leases)
	emit("locks", "gauge", "held or contended locks", fs.Locks)
	emit("sessions", "gauge", "client write sessions", fs.Sessions)
	emit("watchers", "gauge", "active watchers", fs.Watchers)
	if s.wal != nil {
		emit("wal_fsync_total", "counter", "fsync calls issued by the WAL", s.wal.SyncCount())
		emit("wal_bytes_written_total", "counter", "bytes appended to the WAL", s.wal.BytesWritten())
		emit("wal_segments", "gauge", "WAL segment files on disk", s.wal.SegmentCount())
	}
}
