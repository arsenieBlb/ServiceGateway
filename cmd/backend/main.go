// backend is a loopback-only demonstration service, including deliberate delays.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9001", "loopback listen address")
	id := flag.String("id", "backend-a", "backend identifier")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		log.Fatal("listen must contain a numeric loopback IP and port")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		delay, err := time.ParseDuration(r.URL.Query().Get("delay"))
		if r.URL.Query().Get("delay") != "" && (err != nil || delay < 0 || delay > 30*time.Second) {
			http.Error(w, "delay must be between 0 and 30s", http.StatusBadRequest)
			return
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
		if err != nil {
			return
		}
		status := http.StatusOK
		if value := r.URL.Query().Get("status"); value != "" {
			status, err = strconv.Atoi(value)
			if err != nil || status < 200 || status > 599 {
				http.Error(w, "invalid status", 400)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-ID", *id)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"backend": *id, "method": r.Method, "uri": r.RequestURI, "body": string(body)})
	})
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		deadline, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(deadline)
	}()
	fmt.Fprintf(os.Stderr, "backend %s listening on %s\n", *id, *listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	stop()
	<-done
}
