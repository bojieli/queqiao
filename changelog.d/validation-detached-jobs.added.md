Live validation campaigns keep running their test jobs through a dropped
connection to a test host, and a block whose job host stays unreachable is set
aside for both arms instead of leaving the whole campaign without a verdict; a
job that hangs past its window or fails on a reachable host counts against the
arm. Campaigns spend less time between measurement windows (one shared ssh
connection per host, parallel restarts and scrapes, faster polling).
