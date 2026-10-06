Serialize bulk pool idle-timer ownership with borrowing, release and reset.
Late releases cannot rearm detached connections, and callbacks from older
idle periods cannot close a newly reused connection. Transport closure is
idempotent when reset and release race.
