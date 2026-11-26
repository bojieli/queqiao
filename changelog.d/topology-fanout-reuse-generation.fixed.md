With `--topology=single --topology-fanout`, a flow that already has a
fan-out lane now judges whether its control lane is shared on that lane's
own pooled generation, as opening a fan-out lane already did. After a
handover, a flow draining on the old generation no longer sends a lone bulk
flow's chunks from its warm control lane to its fan-out lane. Isolation
keeps the client-wide count.
