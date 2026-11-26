`--congestion-scope=path`: when a probe timeout demotes a connection and the
demotion publishes new loss figures, the pacing balance is settled at the
probe's own send time, so the interval up to the next event is no longer
credited twice.
