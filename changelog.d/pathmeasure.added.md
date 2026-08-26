`pathmeasure`, a companion to `pathprobe` that measures what a stack achieves
rather than what a path does. It reports flow completion time for
request-sized payloads, shows what a long-lived connection keeps between
bursts, measures both directions, runs concurrent request and frame workloads,
and reads the kernel's TCP_INFO to say which constraint was binding.
