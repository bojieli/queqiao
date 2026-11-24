A burst of QUIC lanes joining a shared path that had gone quiet no longer
stays capped near the trickle the path was carrying: each joining lane now
ramps from its seed until the path shows a queue, loss that rises with its own
sending rate, or delivery that stops growing, and only then takes its share of
the pool. A lane judges its loss against its own first rounds once a faster
round confirms them, so a floor inherited from an older measurement no longer
ends the ramp or blocks compensation on a lossier path, and the ramp itself
earns the erasure compensation it showed. A lane ramps again when lanes beside
it finish, and a lane is no longer capped when it is the only one actually
using the path. On a fast path with moderate erasure this restores the
goodput the honest pooled seed had cost, and on a lossy path it no longer
decays to a third of capacity.

A QUIC lane no longer takes on more than it can deliver in four of its own round
trips, so a slow lane stops holding chunks long enough for them to be copied
to another lane; and rescue lanes left idle after a stall no longer each take
a share of the path from the lanes carrying data.
