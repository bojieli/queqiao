A campaign reports a start that leaves no pid file at the start, with the
command's own output, and retries it once after a dropped session, instead
of the next wait reporting "no pid file"; the wait now gives the pid file
a few seconds to appear before calling a start failed.
