Deterministic CI tests for the workloads the stage-10 gate moved out of live
runs (`docs/WORKLOAD-MODEL.md`): long-idle survival on an injected clock
(W9b), a tunnelled QUIC download and page load (W8), heavy UDP beside thin
UDP and coded chunks (M5), segment fetch (W10) and one-shot datagrams (W11).
Each runs the legacy bundle and the flip bundle through a real client and
gateway on the same seeded emulated path and applies the model's bounds.
