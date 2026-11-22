package mobilecore

import (
	"fmt"
	"image"
	"strings"
	"unicode/utf8"

	"github.com/liyue201/goqr"
)

// maxQRFramePixels bounds the frame a caller may hand to DecodeQRCode. A
// camera preview frame is one or two megapixels; anything larger is a caller
// mistake, and would only make each decode slower without reading a code the
// smaller frame could not.
const maxQRFramePixels = 8 << 20

// DecodeQRCode reads one QR code out of an 8-bit luminance frame such as the
// Y plane of an Android YUV_420_888 image: width*height bytes, row-major, no
// padding. The frame is decoded in process, so an invitation never leaves the
// device as an image.
//
// It returns the decoded text, or an empty string when the frame holds no
// readable code, which is the normal result for most frames of a live camera
// preview and is therefore not an error. When several codes are visible, the
// one carrying a queqiao:// invitation wins. An error means the frame itself
// is malformed.
func DecodeQRCode(luma []byte, width, height int) (text string, err error) {
	if width <= 0 || height <= 0 {
		return "", fmt.Errorf("qr frame dimensions %dx%d are not positive", width, height)
	}
	if width > maxQRFramePixels/height {
		return "", fmt.Errorf("qr frame %dx%d exceeds %d pixels", width, height, maxQRFramePixels)
	}
	if len(luma) != width*height {
		return "", fmt.Errorf("qr frame has %d bytes, want %d for %dx%d", len(luma), width*height, width, height)
	}
	// The decoder is a port of C code that has not had to face every frame a
	// phone camera can produce. A panic on one frame must read as "no code in
	// this frame", not take the application down mid-import.
	defer func() {
		if recovered := recover(); recovered != nil {
			text, err = "", nil
		}
	}()
	frame := &image.Gray{Pix: luma, Stride: width, Rect: image.Rect(0, 0, width, height)}
	codes, decodeErr := goqr.Recognize(frame)
	if decodeErr != nil {
		// goqr reports an empty frame as an error; the caller cannot act on
		// the distinction between "nothing there" and "something unreadable".
		return "", nil
	}
	var first string
	for _, code := range codes {
		if code == nil || len(code.Payload) == 0 || !utf8.Valid(code.Payload) {
			continue
		}
		payload := string(code.Payload)
		if strings.HasPrefix(strings.TrimSpace(payload), invitationScheme) {
			return payload, nil
		}
		if first == "" {
			first = payload
		}
	}
	return first, nil
}

const invitationScheme = "queqiao://"
