A gateway whose session limit was full no longer refuses the JOIN and PROBE
streams that recover the flows holding that limit (#170). Every slot used to
be taken before a stream's first frame was read, so when established flows
held them all, a flow that lost a lane could not rejoin and failed after its
replacement grace while still holding its slot. A stream that finds no free
slot is now read under a small separate bound, and a JOIN or PROBE for an
existing flow is admitted without one; a full QUIC connection limit admits one
standby connection beside a device's existing ones. New flows are refused as
before.
