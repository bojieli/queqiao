Under `--mem=owned`, the retention autotune samples the credit-round
duration once per round instead of at every read admission, dispatch and
completion, which walked the flow's lanes under their locks while the
scheduler mutex was held. The socket-based idle-reader test runs at an
eighth of the iOS budget, so it fits a 512-descriptor limit.
