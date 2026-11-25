`--carrier-shadow` calibration data is more accurate. Probe-timeout episodes
are counted at the peak the transport reached, from counters the QUIC fork now
keeps as each episode ends, so short episodes and episodes acknowledged
between two samples are no longer undercounted. Carriers used only by UDP
associations are watched too. A client stall's legacy remedy is labelled as
a rescue request (`demote_and_request_rescue`) rather than a dial, and the
summary counts the dials that actually followed. Writing the outage record no
longer delays the gateway's replacement-grace timer. In the summary, skew is
measured from the first client observation of the matched incident, JOINs
started after an outage ended are not counted as transit, episodes still open
when the capture ends are reported as censored, and a later stall in a newer
transport epoch counts as progress.
