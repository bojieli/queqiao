The coded path protects the tail of a burst once, when the producer has
actually paused, instead of at every momentary drain of its send queue. A
response relayed as many small frames was protected afresh after each of
them as though it ended the exchange, which spent up to three repair
datagrams per source symbol -- 75% of the wire for a 100 KB body on a 14%
erasure path, against 21% for the same body sent at once. Source symbols are
still sent immediately; only their tail protection waits, for at most 5 ms
or one fortieth of the round trip.
