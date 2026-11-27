The deterministic workload tests (W8, W9b, W10, W11, M5) now run their flip
arm on the whole default-to-be bundle, `--recovery=carrier` included; the
arm had listed the stage 1-7 switches by hand. The full-suite workflows
allow `go test` 90 minutes, since `internal/pep` with two arms takes about
55 on a 32-core host.
