A QUIC flow rescued onto its TCP fallback lane is no longer closed moments
after the rescue succeeds. When the QUIC lane manager handed the flow off and
returned, it marked replacement abandoned, and the stall watchdog treated that
as terminal while the stalled bytes were still being replayed on the healthy
TCP lane, so long-lived interactive connections such as SSH were reset with
"no replacement lane will be attempted" on sub-second network blips.
Abandonment now ends a flow only when it has no healthy lane; a lane that
never delivers is still bounded by the no-progress budget.
