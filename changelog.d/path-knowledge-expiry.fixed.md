What a path model remembers between connections -- its erasure rate, burst
factor and minimum round trip -- is forgotten after 10 minutes with nothing
reporting. A gateway keeps one model per client address for the life of its
process, so a client returning hours later was seeded with the measurements
of its previous visit, including a round trip floor that only ever moved
down.
