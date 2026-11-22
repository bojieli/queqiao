package pep

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

var (
	errFlowNoProgress          = errors.New("outstanding tunnel bytes made no delivery progress within recovery budget")
	errApplicationBackpressure = errors.New("application stopped reading within delivery budget")
)

func (f *multipathFlow) progressTimeout() time.Duration {
	if f.progressGrace > 0 {
		return f.progressGrace
	}
	return laneReplacementWait
}

// writeApplication bounds only an outstanding write, never application idle
// time. Each successful partial write renews the budget. A consumer that is
// slow but still reading is different from one that has stopped completely.
func (f *multipathFlow) writeApplication(p []byte) error {
	defer f.inner.SetWriteDeadline(time.Time{})
	for len(p) > 0 {
		if err := f.inner.SetWriteDeadline(time.Now().Add(f.progressTimeout())); err != nil {
			return err
		}
		n, err := f.inner.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				return fmt.Errorf("%w: %v", errApplicationBackpressure, err)
			}
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
