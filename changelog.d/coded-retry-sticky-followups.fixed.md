Under `--coded-retry=sticky`, receiver gap evidence is dated by when the
acknowledgement that advanced it arrived, not by when the scheduler's scan
ran, so an acknowledgement older than a coded packet can no longer cause a
premature stream retry. Every gap-driven retry now counts in
`queqiao_lane_reinjections_total`, including one that a late registration
or write report completes. A coded first attempt no longer looks up a
recovery deadline at dispatch that it would discard.
