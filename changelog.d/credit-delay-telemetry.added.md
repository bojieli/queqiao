Every endpoint now exports sender-side credit delay as the
`queqiao_credit_delay_seconds` histogram on `/metrics`: the time from a
chunk's last byte being taken by the carrier to the flow acknowledgement
that covers it, minus the path's minimum round trip, on the sender's own
clock. It is passive and needs no flag. The validation harness reads it,
and the shadow false-promotion rate from the carrier-shadow counters, as
generic secondaries: gated in every trial type at stage 10 and reported at
stages 6 and 7. The carrier-shadow summary also prints the false-promotion
rate.
