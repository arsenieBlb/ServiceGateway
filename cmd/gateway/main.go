package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"service-gateway/internal/config"
	"service-gateway/internal/gateway"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "demo/example.json", "validated JSON configuration")
	flag.Parse()
	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	c, err := config.Load(f)
	f.Close()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	g, err := gateway.New(c, logger)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	adminListener, err := net.Listen("tcp", c.AdminListen)
	if err != nil {
		return err
	}
	defer adminListener.Close()
	server := &http.Server{Handler: g.Handler(), ReadHeaderTimeout: time.Duration(c.Timeouts.ReadHeader), ReadTimeout: time.Duration(c.Timeouts.ReadBody), IdleTimeout: time.Duration(c.Timeouts.Idle), MaxHeaderBytes: c.Limits.MaxHeaderBytes}
	admin := &http.Server{Handler: g.AdminHandler(), ReadHeaderTimeout: time.Duration(c.Timeouts.ReadHeader), ReadTimeout: time.Duration(c.Timeouts.ReadBody), WriteTimeout: time.Duration(c.Timeouts.Request), IdleTimeout: time.Duration(c.Timeouts.Idle), MaxHeaderBytes: c.Limits.MaxHeaderBytes}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g.StartHealth(ctx)
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(listener) }()
	go func() { errs <- admin.Serve(adminListener) }()
	logger.Info("listening", "public", listener.Addr().String(), "admin", adminListener.Addr().String())
	select {
	case <-ctx.Done():
	case err = <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), time.Duration(c.Timeouts.Shutdown))
	defer cancel()
	// Stop admission first; Server.Shutdown alone permits existing handlers to queue.
	gdone := make(chan error, 1)
	go func() { gdone <- g.Shutdown(shutdown) }()
	serr := server.Shutdown(shutdown)
	if serr != nil {
		server.Close()
	}
	aerr := admin.Shutdown(shutdown)
	if aerr != nil {
		admin.Close()
	}
	gerr := <-gdone
	if err != nil {
		return err
	}
	if gerr != nil {
		return gerr
	}
	if serr != nil {
		return serr
	}
	return aerr
}
