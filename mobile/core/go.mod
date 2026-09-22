module github.com/bojieli/queqiao/mobile/core

go 1.26.6

require (
	github.com/bojieli/queqiao v0.0.0
	github.com/liyue201/goqr v0.0.0-20200803022322-df443203d4ea
	golang.org/x/sys v0.47.0
	gvisor.dev/gvisor v0.0.0-20260701204157-69c2d17aea96
)

require (
	github.com/andybalholm/brotli v1.0.6 // indirect
	github.com/apernet/quic-go v0.61.1-0.20260806010916-184d081eef3e // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/refraction-networking/utls v1.8.2 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20250711185948-6ae5c78190dc // indirect
	golang.org/x/mobile v0.0.0-20260816165457-f98cc9b3c733 // indirect
	golang.org/x/mod v0.39.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.49.0 // indirect
)

// Gomobile requires this directive in the target module. Its command-only
// dependencies are audited separately. The x/mobile binding support added by
// gomobile is also covered by the runtime lock and final-binary audit.
tool golang.org/x/mobile/cmd/gobind

replace github.com/bojieli/queqiao => ../..
