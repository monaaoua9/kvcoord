FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvd ./cmd/kvd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvctl ./cmd/kvctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvd /out/kvctl /usr/local/bin/
VOLUME /data
EXPOSE 7001 9100
ENTRYPOINT ["/usr/local/bin/kvd"]
