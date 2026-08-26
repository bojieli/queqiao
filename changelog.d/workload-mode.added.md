`pathmeasure -mode workload` drives a real HTTP endpoint against two arms
rather than a transfer of the same size, splitting each request into connect,
request-to-first-byte, and download so that server-side compute does not bury
what the transport did. On a request shape where the leg containing the server
should match between arms, it checks that it does and says so.
