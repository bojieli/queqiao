Builds now use Go 1.26.11, which fixes a net/http vulnerability
(GO-2026-6859), and the indirect compression dependencies are upgraded to
github.com/andybalholm/brotli v1.2.7 (GO-2026-6888) and
github.com/klauspost/compress v1.20.2 (GO-2026-6865). govulncheck reported all
three as reachable from the mobile core; the desktop binary links the same
modules.
