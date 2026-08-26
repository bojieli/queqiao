`--flow-metadata-socket` lets the client ask a local capture agent what
produced each flow, so its class is known before it carries anything instead
of after the second the classifier needs to decide. A request that finishes
in 200ms spends its whole life inside that second. The profile maps what the
agent reports, usually an executable path, to a starting class; the
classifier still judges the flow by what it then does. Without the flag, and
without a matching hint, nothing changes.
