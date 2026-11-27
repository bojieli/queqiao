`queqiaod relay run --idle-timeout` is now an idle timeout for TCP
connections, as it already was for UDP associations. It used to be an
absolute lifetime: every TLS/TCP lane or TCP handoff through a relay was cut
that long after it was accepted (two minutes by default) however busy it
was. A forwarded TCP connection is now closed only when no byte has crossed
it in either direction for the timeout.
