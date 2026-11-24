A flow the client host slept through for longer than the gateway holds a
silent flow now fails with `peer dropped this session while this host was
suspended` instead of `peer does not hold this session`, and its completion
record carries `host_suspended`. The gateway had ended these flows correctly
during the sleep; they were being counted as sessions it lost.
