The stage 9 DC gate now measures a leaf's capacity for its binding check as
the larger of the arms' median solo rates (`dc_gate.ini [validity]
binding_reference`, frozen), because capacity belongs to the path, and a
legacy arm that cannot fill a lone leaf made a binding ancestor look unbound.
W4's transactional requests run on absolute slots from the phase's barrier.
A request that has to wait for the previous one is late, and its latency is
measured from its slot. One slow request therefore counts against its arm
instead of shifting the leaf's later waves until the trial goes unmeasured.
