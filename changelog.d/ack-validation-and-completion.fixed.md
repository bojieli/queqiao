Validate complete ACK frames and selective ranges before advancing send state.
Repeated or empty selective acknowledgements no longer reset stall progress
or clear lane suspicion. Release the receive worker promptly when local
sending completes after the remote FIN.
