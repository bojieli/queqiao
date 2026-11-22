package main

import (
	"errors"
	"fmt"
	"strings"

	qrcode "github.com/skip2/go-qrcode"
)

// ANSI colours pin the code to black modules on a white field. Without them a
// terminal with a dark theme shows the code inverted, and not every scanner
// reads an inverted code; the mobile apps' decoder does not.
const (
	qrRowPrefix = "\x1b[30;47m"
	qrRowSuffix = "\x1b[0m"
)

// renderQRCode draws text as a QR code for a terminal, two modules per
// character row using half blocks so a full-size invitation fits a normal
// window. The four-module quiet zone the symbol needs comes from the encoder
// and is drawn in white like everything else, so the code stands on its own
// field whatever the terminal background is.
func renderQRCode(text string) (string, error) {
	code, err := qrcode.New(text, qrcode.Medium)
	if err != nil {
		return "", fmt.Errorf("encode qr code: %w", err)
	}
	modules := code.Bitmap()
	if len(modules) == 0 {
		return "", errors.New("encode qr code: empty symbol")
	}
	var out strings.Builder
	for top := 0; top < len(modules); top += 2 {
		out.WriteString(qrRowPrefix)
		for column := range modules[top] {
			upper := modules[top][column]
			lower := false
			if top+1 < len(modules) {
				lower = modules[top+1][column]
			}
			out.WriteString(halfBlock(upper, lower))
		}
		out.WriteString(qrRowSuffix)
		out.WriteByte('\n')
	}
	return out.String(), nil
}

// halfBlock draws two vertically adjacent modules as one character cell in
// black-on-white: the foreground paints the dark modules, the field is white.
func halfBlock(upperDark, lowerDark bool) string {
	switch {
	case upperDark && lowerDark:
		return "█"
	case upperDark:
		return "▀"
	case lowerDark:
		return "▄"
	default:
		return " "
	}
}
