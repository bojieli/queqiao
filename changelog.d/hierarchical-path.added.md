A path can be modeled as a chain of segments (uplink, then peer) instead of
a single endpoint pair, so flows to different places pool what they share
and keep separate what they don't. The `dc-long-haul` profile enables it;
the access-link profile is unchanged.
