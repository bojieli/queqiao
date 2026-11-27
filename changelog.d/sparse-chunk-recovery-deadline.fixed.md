A small write that reached an empty send queue is recovered in half a
round trip instead of two when a shaper drops it, so a keystroke's echo no
longer waits about 500 ms behind a bulk transfer on a policer (its median
is unchanged). Chunks with data behind them keep the longer deadline.
