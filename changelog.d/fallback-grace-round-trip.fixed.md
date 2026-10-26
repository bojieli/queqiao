The adaptive QUIC preference window (`--fallback-grace-rtt-factor`) now
scales the connection's measured round trip rather than how long the last
dial took. A fresh dial is a handshake and an authentication, several round
trips, so after any cold dial the window sat at its 30 second ceiling and
every request made while the pool was down waited that long for QUIC with a
TCP lane already established; a 200 ms path now gets 3.2 seconds.
