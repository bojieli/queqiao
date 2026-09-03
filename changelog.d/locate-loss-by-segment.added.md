`queqiaod segments` profiles a live tunnel and says which segment the loss is
on, rather than only how much there is. A slow deployment has three candidates
that need three different responses -- the client's own access link, the long
haul this transport carries, and the gateway's transit onward -- and until now
every instrument here described the path end to end, so an operator losing a
seventh of what crossed the path learned only that they were losing a seventh.
The command measures both ends in the same minutes and localises the fault by
what the legs share: it reads the running client's own per-direction erasure
from `--metrics-listen`, which is the only measurement that separates upstream
from downstream, and with `--ssh` it runs the same code on the gateway to
measure that end's transit directly instead of inferring it by subtraction.
Anchors are chosen per vantage point and never averaged, because a probe from
a filtered network to a filtered destination measures the filter: a Chinese
and a global anchor are probed from both ends, whichever answers cleanly
establishes
that end's own link, and an address that answers echo while refusing a
handshake is reported as filtering rather than charged to anyone's link. A leg
that returns nothing is reported as unanswered, not as total loss.
