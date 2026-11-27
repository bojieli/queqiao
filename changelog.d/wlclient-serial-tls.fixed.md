The validation workload client no longer reads and writes one TLS connection
from two threads at once. Python's ssl module does not serialise those calls,
and the terminal and idle-wake workloads could end a session with a spurious
EOF (or "bad record mac") that the arm under test never caused.
