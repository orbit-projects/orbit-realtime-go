// Copyright 2026-present Orbit Contributors.
// Licensed under the Apache License, Version 2.0.

package realtime_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	realtime "github.com/orbit-projects/orbit-realtime-go"
)

func TestEventEncodingAndBounds(t *testing.T) {
	retry := uint32(2500)
	frame, err := (realtime.Event{Event: "change", ID: "42", Retry: &retry, Data: "first\r\nsecond"}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := "event: change\nid: 42\nretry: 2500\ndata: first\ndata: second\n\n"
	if string(frame) != want {
		t.Fatalf("frame mismatch: %q", frame)
	}
	if _, err := (realtime.Event{Event: "bad\nevent", Data: "x"}).Encode(); err == nil {
		t.Fatal("expected newline rejection")
	}
	if _, err := (realtime.Event{ID: "bad\x00id", Data: "x"}).Encode(); err == nil {
		t.Fatal("expected NUL rejection")
	}
	if _, err := (realtime.Event{Data: strings.Repeat("é", realtime.MaxEventBytes)}).Encode(); err == nil {
		t.Fatal("expected encoded-size rejection")
	}
	if strings.Contains((realtime.Event{Data: "private", ID: "secret"}).String(), "private") {
		t.Fatal("String exposed payload")
	}
}

func TestHandlerStreamsAndFlushes(t *testing.T) {
	h, err := realtime.NewHandler(func(ctx context.Context, emit realtime.Emit) error {
		return emit(realtime.Event{Data: "hello"})
	})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest("GET", "/events", nil))
	if got := r.Body.String(); got != "data: hello\n\n" {
		t.Fatalf("body = %q", got)
	}
	if got := r.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if got := r.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("cache control = %q", got)
	}
	if !r.Flushed {
		t.Fatal("event was not flushed")
	}
}

func TestHandlerObservesCancellation(t *testing.T) {
	called := false
	h, _ := realtime.NewHandler(func(ctx context.Context, emit realtime.Emit) error {
		called = true
		return emit(realtime.Event{Data: "ok"})
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
	if !called {
		t.Fatal("producer was not invoked")
	}
	if r.Body.Len() != 0 {
		t.Fatal("emitted after cancellation")
	}
}

func TestHandlerSanitizesProducerFailureBeforeStreaming(t *testing.T) {
	h, err := realtime.NewHandler(func(context.Context, realtime.Emit) error {
		return errors.New("private provider details")
	})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest("GET", "/events", nil))
	if r.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", r.Code)
	}
	if strings.Contains(r.Body.String(), "private provider details") {
		t.Fatal("response leaked producer error")
	}
}
