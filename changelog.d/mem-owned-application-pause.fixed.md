Under `--mem=owned` a flow keeps its retention bound through a pause of its
application, for up to five minutes. The bound used to decay through every
idle round to the startup allowance, so a flow that sent a burst every few
seconds, such as a video player fetching segments, slow-started each burst
again; on an emulated long-haul path its slowest segments ran at three
quarters of the legacy bundle's throughput.
