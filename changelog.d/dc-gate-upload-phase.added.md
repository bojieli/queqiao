The stage 9 DC gate campaign now has an upload phase that can see the
`dc-long-haul` path tree's joint limit. Several leaves upload at once through
one multi-provider client process (`queqiaod client --providers`). Each
provider reaches the gateway through a relay on its own host, so each is its
own leaf, and the uploader's uplink is their shared ancestor. A light sibling
runs beside them. `dc_gate.py` judges the phase on the section 11.3 fairness
criteria, with its own bounds frozen in `dc_gate.ini` `[upload]`. It checks
that the uplink bound and that every leaf had its own relay. The design
document records the lead's decisions in #235 on fan-out, the authority key,
the initial pacing hint and the relay evidence.
