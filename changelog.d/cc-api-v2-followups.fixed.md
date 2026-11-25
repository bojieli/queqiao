Under `--cc-api=v2`, a reissued verification grant takes effect as soon as it
is bound, a replacement controller no longer inherits its predecessor's grant,
and a send pass that ends without a packet refreshes the connection's demand,
so a discarded datagram or a write that hit its limit no longer leaves a stale
class asserted until an unrelated event.
