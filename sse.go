// Copyright 2026-present Orbit Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package realtime provides bounded Server-Sent Events framing and net/http streaming.
package realtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	MaxEventBytes     = 65_536
	MaxEventNameBytes = 256
	MaxEventIDBytes   = 1_024
	MaxCommentBytes   = 4_096
	MaxRetryMS        = 2_147_483_647
)

var (
	ErrInvalidEvent = errors.New("invalid SSE event")
)

// Event is one bounded SSE event. Data and ID are excluded from String output.
type Event struct {
	Data  string
	Event string
	ID    string
	Retry *uint32
}

// String deliberately omits user-controlled event data and IDs.
func (e Event) String() string {
	return fmt.Sprintf("Event{event_set:%t data_bytes:%d id_set:%t}", e.Event != "", len(e.Data), e.ID != "")
}

// Encode serializes one event according to the HTML event-stream format.
func (e Event) Encode() ([]byte, error) {
	if err := validateText(e.Data, MaxEventBytes, false, false); err != nil {
		return nil, err
	}
	if e.Event != "" {
		if err := validateText(e.Event, MaxEventNameBytes, true, false); err != nil {
			return nil, err
		}
	}
	if e.ID != "" {
		if err := validateText(e.ID, MaxEventIDBytes, true, true); err != nil {
			return nil, err
		}
	}
	if e.Retry != nil && *e.Retry > MaxRetryMS {
		return nil, fmt.Errorf("%w: retry is outside supported range", ErrInvalidEvent)
	}
	var b strings.Builder
	if e.Event != "" {
		b.WriteString("event: ")
		b.WriteString(e.Event)
		b.WriteByte('\n')
	}
	if e.ID != "" {
		b.WriteString("id: ")
		b.WriteString(e.ID)
		b.WriteByte('\n')
	}
	if e.Retry != nil {
		fmt.Fprintf(&b, "retry: %d\n", *e.Retry)
	}
	data := strings.ReplaceAll(strings.ReplaceAll(e.Data, "\r\n", "\n"), "\r", "\n")
	for _, line := range strings.Split(data, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	if b.Len() > MaxEventBytes {
		return nil, fmt.Errorf("%w: encoded event exceeds %d-byte limit", ErrInvalidEvent, MaxEventBytes)
	}
	return []byte(b.String()), nil
}

// EncodeComment returns a bounded SSE comment frame suitable for explicit keepalives.
func EncodeComment(comment string) ([]byte, error) {
	if err := validateText(comment, MaxCommentBytes, false, false); err != nil {
		return nil, err
	}
	normalized := strings.ReplaceAll(strings.ReplaceAll(comment, "\r\n", "\n"), "\r", "\n")
	var b strings.Builder
	for _, line := range strings.Split(normalized, "\n") {
		b.WriteString(": ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	if b.Len() > MaxCommentBytes {
		return nil, fmt.Errorf("%w: encoded comment exceeds %d-byte limit", ErrInvalidEvent, MaxCommentBytes)
	}
	return []byte(b.String()), nil
}

func validateText(value string, max int, lineSafe, rejectNull bool) error {
	if !utf8.ValidString(value) || len(value) > max || (lineSafe && strings.ContainsAny(value, "\r\n")) || (rejectNull && strings.ContainsRune(value, '\x00')) {
		return fmt.Errorf("%w: text is invalid or exceeds its bound", ErrInvalidEvent)
	}
	return nil
}

// Emit writes one event and flushes it before returning, applying downstream backpressure directly.
type Emit func(Event) error

// Stream produces events synchronously. It must return when ctx is canceled; emit fails after disconnect.
type Stream func(ctx context.Context, emit Emit) error

// NewHandler adapts a producer to net/http without an unbounded intermediate queue.
func NewHandler(stream Stream) (http.Handler, error) {
	if stream == nil {
		return nil, fmt.Errorf("%w: stream is required", ErrInvalidEvent)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		controller := http.NewResponseController(w)
		committed := false
		emit := func(event Event) error {
			if err := r.Context().Err(); err != nil {
				return err
			}
			frame, err := event.Encode()
			if err != nil {
				return err
			}
			committed = true
			if _, err := w.Write(frame); err != nil {
				return err
			}
			return controller.Flush()
		}
		if err := stream(r.Context(), emit); err != nil && r.Context().Err() == nil && !committed {
			// Before the first frame, return a generic error without exposing producer details.
			http.Error(w, "event stream unavailable", http.StatusInternalServerError)
		}
	}), nil
}
