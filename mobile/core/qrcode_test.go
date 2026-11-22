package mobilecore

import (
	"image"
	"image/png"
	"os"
	"strings"
	"testing"
)

// The fixture is a 512x512 grayscale PNG of this payload, generated once with
// an unrelated QR encoder so the test does not depend on the decoder under
// test agreeing with itself. It is the length of a real invitation, whose
// JSON body is a few hundred bytes of base64url.
var fixturePayload = "queqiao://enroll/" + strings.Repeat("Ab3_-", 100)

func loadGrayFixture(t *testing.T, path string) *image.Gray {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoded, err := png.Decode(file)
	if err != nil {
		t.Fatal(err)
	}
	gray, ok := decoded.(*image.Gray)
	if !ok {
		t.Fatalf("fixture %s decoded as %T, want *image.Gray", path, decoded)
	}
	return gray
}

// placeInFrame draws the code into a larger, unevenly lit frame the way a
// camera would see it: off-centre, smaller than the frame, on a background
// that is neither black nor white. rotate turns it a quarter turn, which a
// hand-held phone does all the time.
func placeInFrame(code *image.Gray, width, height, left, top int, rotate bool) []byte {
	luma := make([]byte, width*height)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			luma[y*width+x] = byte(120 + 80*x/width)
		}
	}
	size := code.Rect.Dx()
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			sx, sy := x, y
			if rotate {
				sx, sy = y, size-1-x
			}
			luma[(top+y)*width+left+x] = code.Pix[sy*code.Stride+sx]
		}
	}
	return luma
}

func TestDecodeQRCodeReadsInvitationFromCameraFrame(t *testing.T) {
	if len(fixturePayload) != 517 {
		t.Fatalf("fixture payload is %d bytes; the PNG was generated from 517", len(fixturePayload))
	}
	code := loadGrayFixture(t, "testdata/invitation-qr.png")
	for _, test := range []struct {
		name          string
		width, height int
		left, top     int
		rotate        bool
	}{
		{name: "landscape", width: 1280, height: 720, left: 400, top: 100},
		{name: "portrait", width: 720, height: 1280, left: 100, top: 380},
		{name: "rotated quarter turn", width: 1280, height: 720, left: 300, top: 60, rotate: true},
		{name: "exact", width: 512, height: 512},
	} {
		t.Run(test.name, func(t *testing.T) {
			luma := placeInFrame(code, test.width, test.height, test.left, test.top, test.rotate)
			got, err := DecodeQRCode(luma, test.width, test.height)
			if err != nil {
				t.Fatal(err)
			}
			if got != fixturePayload {
				t.Fatalf("DecodeQRCode = %q, want the fixture payload", got)
			}
		})
	}
}

func TestDecodeQRCodeEmptyFrameIsNotAnError(t *testing.T) {
	luma := make([]byte, 640*480)
	for i := range luma {
		luma[i] = byte(90 + (i*7)%60)
	}
	got, err := DecodeQRCode(luma, 640, 480)
	if err != nil {
		t.Fatalf("an empty preview frame is the common case and must not raise: %v", err)
	}
	if got != "" {
		t.Fatalf("DecodeQRCode on noise = %q, want empty", got)
	}
}

func TestDecodeQRCodeRejectsMalformedFrames(t *testing.T) {
	for _, test := range []struct {
		name          string
		bytes         int
		width, height int
	}{
		{name: "zero width", bytes: 0, width: 0, height: 10},
		{name: "negative height", bytes: 0, width: 10, height: -1},
		{name: "short buffer", bytes: 99, width: 10, height: 10},
		{name: "long buffer", bytes: 101, width: 10, height: 10},
		{name: "oversized frame", bytes: 0, width: 1 << 20, height: 1 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeQRCode(make([]byte, test.bytes), test.width, test.height)
			if err == nil {
				t.Fatal("DecodeQRCode accepted a malformed frame")
			}
			if strings.Contains(err.Error(), "panic") {
				t.Fatalf("malformed input reached the decoder: %v", err)
			}
		})
	}
}
