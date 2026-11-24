A TCP flow whose only lane ends now always gets its zero-lane replacement
attempts. Stall rescues beside a still-open lane used to spend the same
budget, so a flow rescued eight times within five minutes lost its last lane
with nothing left and waited out the whole 45-second replacement grace before
failing with `lane replacement timeout` and `lane_replacement_attempts` 0. A
spent zero-lane budget now also fails the flow at once, as on QUIC, instead
of holding the application for a grace nothing will arrive during.
