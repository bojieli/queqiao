A burst of QUIC lanes joining a shared path that had gone quiet no longer
stays capped near the trickle the path was carrying: each joining lane now
ramps from its seed until the path shows a queue, loss above its measured
erasure, or delivery that stops growing, and only then takes its share of the
pool. On a fast path with moderate erasure this restores the goodput the
honest pooled seed had cost.
