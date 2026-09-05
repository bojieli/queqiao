Lane rescue JOINs at the gateway are no longer blocked by the control/bulk
role restriction when a flow's lane ceiling is reached: eviction now picks
the oldest evictable lane, preferring one with the replacement's role but
never refusing a rescue for want of a same-role victim, so a dead control
lane can no longer wedge a flow's last admission slot. A JOIN lane whose
OPEN_OK could not be written is also removed from the flow at once instead
of leaking an admission slot that could never carry traffic. Lanes whose
handshake is in flight or younger than the rescue-race window remain
unevictable, since evicting them can kill the rescue attempt the client has
just crowned.
