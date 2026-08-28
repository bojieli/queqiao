The iOS packet-tunnel extension no longer runs itself into the memory ceiling
NetworkExtension enforces on it. The Go runtime soft limit was 40 MiB inside a
50 MiB whole-process cap, which left too little of that cap for everything the
Go heap does not cover: the resident text pages of the statically linked Go
and gVisor code, runtime metadata, goroutine and thread stacks, the Swift
packet bridge, and CoreFoundation. The collector spent whole minutes at
130-200% CPU defending a limit the process could not honour, and jetsam killed
the extension anyway, twelve to twenty-one minutes into a session. Because the
kill is a SIGKILL, the provider never ran `stopTunnel` and never recorded a
diagnostic, so the tunnel's network settings stayed installed and the VPN went
on showing connected while no packets moved — the failure a user sees is a
stall, with an empty connection log behind it. The limit is now 28 MiB, and
the test suite holds it far enough below the ceiling to leave that remainder
room.
