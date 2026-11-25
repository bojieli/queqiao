`--congestion-scope=path`, the sixth bundle of the path-authority migration,
gives each path and sending direction one congestion controller instead of
one per QUIC connection. Every connection on the path attaches to it and asks
it to admit each packet, so the rate, the window, the erasure estimate, the
minimum round trip and the choice of which class is served next are decided
once for the path. Nothing is divided between connections: there are no
shares, seeds or ramps, and a connection that joins, leaves or idles changes
nothing but the work offered. It works with isolation on, so a client's bulk
and rescue connections, and on a gateway every data connection of one
device, draw on the same budget. Packets that elicit no acknowledgement are
not charged, and the delivery sampler retires its state per packet, so
acknowledgements arriving out of order across connections no longer lose
delivered bytes. It requires `--cc-api=v2` and `--sched=class`; the default
stays `connection`, which behaves as before.
