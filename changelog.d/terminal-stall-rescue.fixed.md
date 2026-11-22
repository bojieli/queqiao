Close the application connection when stall recovery receives a permanent
session rejection or exhausts consecutive lane-capacity refusals. Previously
the lane manager exited with an unresumable flag while a stalled lane still
counted as healthy, leaving the application waiting for a transport timeout.
