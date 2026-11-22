package pep

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"testing"
	"time"
)

// shortFlowAttempt preserves failed attempts separately from completed latency
// samples. ended is when the operation finished, before closing the socket.
type shortFlowAttempt struct {
	started, dialed, ended time.Time
	written, read          int
	err                    error
}

func measureShortFlow(dial func() (net.Conn, error), request, reply []byte) (attempt shortFlowAttempt) {
	attempt.started = time.Now()
	var conn net.Conn
	defer func() {
		attempt.ended = time.Now()
		if conn != nil {
			_ = conn.Close()
		}
	}()
	var err error
	conn, err = dial()
	if err != nil {
		attempt.err = fmt.Errorf("SOCKS dial: %w", err)
		return
	}
	attempt.dialed = time.Now()
	attempt.written, err = conn.Write(request)
	if err == nil && attempt.written != len(request) {
		err = io.ErrShortWrite
	}
	if err != nil {
		attempt.err = fmt.Errorf("request write: %w", err)
		return
	}
	attempt.read, err = io.ReadFull(conn, reply)
	if err != nil {
		attempt.err = fmt.Errorf("reply read: %w", err)
	}
	return
}

func logShortFlowDistribution(t *testing.T, name string, samples []time.Duration) {
	t.Helper()
	if len(samples) == 0 {
		t.Logf("%s: no samples", name)
		return
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	t.Logf("%s: n=%d median=%v p90=%v max=%v; raw attempt-order samples=%v",
		name, len(samples), sorted[len(sorted)/2], sorted[(len(sorted)-1)*9/10], sorted[len(sorted)-1], samples)
}

// A measurement helper must never turn a write failure, an early EOF, or a
// partial response into a completed sample. In particular, closing an OPEN
// early must fail the test rather than improve a median of the survivors.
func TestShortFlowMeasurementPreservesFailures(t *testing.T) {
	const requestBytes, replyBytes = 16, 1400
	broken := errors.New("connection broken")
	for _, tc := range []struct {
		name        string
		dialErr     error
		writeErr    error
		writeBytes  int
		replyBytes  int
		wantErr     error
		wantWritten int
		wantRead    int
	}{
		{name: "dial failure", dialErr: broken, wantErr: broken},
		{name: "write failure", writeErr: broken, wantErr: broken},
		{name: "short write", writeBytes: 3, wantErr: io.ErrShortWrite, wantWritten: 3},
		{name: "early close", writeBytes: requestBytes, wantErr: io.EOF, wantWritten: requestBytes},
		{name: "partial reply", writeBytes: requestBytes, replyBytes: 5, wantErr: io.ErrUnexpectedEOF, wantWritten: requestBytes, wantRead: 5},
		{name: "complete", writeBytes: requestBytes, replyBytes: replyBytes, wantWritten: requestBytes, wantRead: replyBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &shortFlowMeasurementConn{
				reader:     bytes.NewReader(make([]byte, tc.replyBytes)),
				writeBytes: tc.writeBytes, writeErr: tc.writeErr,
			}
			attempt := measureShortFlow(func() (net.Conn, error) {
				if tc.dialErr != nil {
					return nil, tc.dialErr
				}
				return conn, nil
			}, make([]byte, requestBytes), make([]byte, replyBytes))
			if !errors.Is(attempt.err, tc.wantErr) {
				t.Fatalf("measurement error = %v, want %v", attempt.err, tc.wantErr)
			}
			if attempt.written != tc.wantWritten || attempt.read != tc.wantRead {
				t.Errorf("measured request/reply bytes = %d/%d, want %d/%d", attempt.written, attempt.read, tc.wantWritten, tc.wantRead)
			}
			if attempt.started.IsZero() || attempt.ended.Before(attempt.started) {
				t.Errorf("attempt did not retain its elapsed-time interval: %+v", attempt)
			}
			if tc.dialErr != nil {
				if !attempt.dialed.IsZero() {
					t.Error("failed SOCKS dial was recorded as acknowledged")
				}
			} else {
				if attempt.dialed.Before(attempt.started) || attempt.dialed.After(attempt.ended) {
					t.Errorf("SOCKS acknowledgement is outside the attempt interval: %+v", attempt)
				}
				if !conn.closed {
					t.Error("measurement left the connection open")
				}
			}
		})
	}
}

type shortFlowMeasurementConn struct {
	net.Conn
	reader     io.Reader
	writeBytes int
	writeErr   error
	closed     bool
}

func (c *shortFlowMeasurementConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (c *shortFlowMeasurementConn) Write([]byte) (int, error)  { return c.writeBytes, c.writeErr }
func (c *shortFlowMeasurementConn) Close() error {
	c.closed = true
	return nil
}
