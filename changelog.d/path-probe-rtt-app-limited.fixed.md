Under `--congestion=path`, an application-limited path, such as the uplink
beside a download, no longer takes a ProbeRTT every ten seconds. The ProbeRTT
held the path's window at four packets for a round trip and 200 ms, which
delayed every datagram and acknowledgement on it by up to about 350 ms: voice
packets sent beside a tunnelled download were late by that much, and the
download stalled while its acknowledgements waited. A path whose window or
pacer held it back since its minimum RTT was measured, as a bulk one does,
still takes its ProbeRTT.
