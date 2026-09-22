# Third-party notices

Queqiao is licensed under the MIT License. The `queqiaod` binary also links the
following Go modules:

| Module | Version | License |
| --- | --- | --- |
| `github.com/andybalholm/brotli` | v1.0.6 | MIT |
| `github.com/apernet/quic-go` | v0.61.1-0.20260806010916-184d081eef3e | MIT |
| `github.com/klauspost/compress` | v1.18.7 | BSD-3-Clause |
| `github.com/refraction-networking/utls` | v1.8.2 | BSD-3-Clause |
| `github.com/skip2/go-qrcode` | v0.0.0-20200617195104-da1b6568686e | MIT |
| `golang.org/x/crypto` | v0.55.0 | BSD-3-Clause |
| `golang.org/x/net` | v0.58.0 | BSD-3-Clause |
| `golang.org/x/sys` | v0.47.0 | BSD-3-Clause |

Every release archive contains `THIRD_PARTY_LICENSES.txt` with the complete
license text read from the exact linked module versions at packaging time. Its
dependency set is derived from the built binary rather than copied from the
larger development module graph. `SBOM.cdx.json` records the same linked
components and their versions.

The congestion-controller design acknowledgements are retained in
`internal/congestion/NOTICE` and included in release archives. Test-only and
benchmark-only dependencies are not linked into `queqiaod` and therefore do
not appear in the binary notice or SBOM.
