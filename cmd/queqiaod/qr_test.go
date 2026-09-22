package main

import (
	"image"
	"strings"
	"testing"

	"github.com/liyue201/goqr"
)

// modulesFromTerminal reads the rendered art back into a module grid, the
// inverse of halfBlock, so the test can rasterize what a terminal would show.
func modulesFromTerminal(t *testing.T, art string) [][]bool {
	t.Helper()
	var modules [][]bool
	for _, line := range strings.Split(strings.TrimSuffix(art, "\n"), "\n") {
		if !strings.HasPrefix(line, qrRowPrefix) || !strings.HasSuffix(line, qrRowSuffix) {
			t.Fatalf("row is not wrapped in the colour escapes: %q", line)
		}
		var upper, lower []bool
		for _, cell := range strings.TrimSuffix(strings.TrimPrefix(line, qrRowPrefix), qrRowSuffix) {
			switch cell {
			case '█':
				upper, lower = append(upper, true), append(lower, true)
			case '▀':
				upper, lower = append(upper, true), append(lower, false)
			case '▄':
				upper, lower = append(upper, false), append(lower, true)
			case ' ':
				upper, lower = append(upper, false), append(lower, false)
			default:
				t.Fatalf("unexpected character %q in rendered code", cell)
			}
		}
		modules = append(modules, upper, lower)
	}
	return modules
}

func TestRenderQRCodeIsReadableByTheMobileDecoder(t *testing.T) {
	uri := "queqiao://enroll/" + strings.Repeat("Ab3_-", 100)
	art, err := renderQRCode(uri)
	if err != nil {
		t.Fatal(err)
	}
	modules := modulesFromTerminal(t, art)
	width := len(modules[0])
	for _, row := range modules {
		if len(row) != width {
			t.Fatalf("rows differ in width: %d and %d", width, len(row))
		}
	}
	for column := 0; column < width; column++ {
		if modules[0][column] || modules[0][width-1-column] {
			t.Fatal("the top row must be quiet zone, or a scanner cannot find the edge")
		}
	}
	for _, row := range modules {
		if row[0] || row[width-1] {
			t.Fatal("the left and right columns must be quiet zone")
		}
	}

	// Rasterize at four pixels per module, the way a terminal font roughly
	// does, and hand it to the decoder the Android app ships.
	const scale = 4
	frame := image.NewGray(image.Rect(0, 0, width*scale, len(modules)*scale))
	for rowIndex, row := range modules {
		for column, dark := range row {
			value := byte(255)
			if dark {
				value = 0
			}
			for dy := 0; dy < scale; dy++ {
				for dx := 0; dx < scale; dx++ {
					frame.Pix[(rowIndex*scale+dy)*frame.Stride+column*scale+dx] = value
				}
			}
		}
	}
	codes, err := goqr.Recognize(frame)
	if err != nil {
		t.Fatalf("the mobile decoder found no code in the terminal rendering: %v", err)
	}
	if len(codes) != 1 || string(codes[0].Payload) != uri {
		t.Fatalf("decoded %d codes, first %q; want the invitation", len(codes), firstPayload(codes))
	}
}

func firstPayload(codes []*goqr.QRData) string {
	if len(codes) == 0 {
		return ""
	}
	return string(codes[0].Payload)
}

func TestRenderQRCodeRefusesWhatNoSymbolCanHold(t *testing.T) {
	if _, err := renderQRCode(strings.Repeat("x", 4000)); err == nil {
		t.Fatal("a 4000-byte payload exceeds the largest QR symbol and must be refused")
	}
}
