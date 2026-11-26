package pep

// The path-authority migration (docs/DESIGN-PATH-AUTHORITY.md §14) ended at
// stage 11 with one architecture and no switches: every QUIC connection
// attaches to its path's PathController, which arbitrates CONTROL,
// INTERACTIVE and BULK (sched=class) and hosts the erasure law through the
// fork's PathSender API (cc-api=v2); coded chunks retry on the stream by
// their registration deadline (coded-retry=sticky); shared memory is owned
// and granted fairly (mem=owned); flows are classified by phase
// (classifier=phased); a client keeps every flow on one connection per path
// (topology=single) and its carrier manager owns QUIC recovery
// (recovery=carrier); a gateway charges its session limit to flows
// (gateway-admission=owned). The legacy bundle, and every switch that chose
// between the two, was deleted with it.

// EffectiveBundle names, in the switch spellings the migration used, what an
// endpoint runs, for its runtime configuration record. Validation tooling
// written during the migration reads the record's bundle field, so it keeps
// its old form: the switches a migration-era build would have had on.
func EffectiveBundle(gateway bool) string {
	if gateway {
		return "cc-api=v2,sched=class,congestion=path,recovery=carrier,coded-retry=sticky,mem=owned,gateway-admission=owned,classifier=phased"
	}
	return "cc-api=v2,sched=class,congestion=path,topology=single,recovery=carrier,coded-retry=sticky,mem=owned,classifier=phased"
}
