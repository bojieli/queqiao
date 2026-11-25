The path-authority gates now have a reproducible live harness.
`scripts/ab_campaign.py` runs interleaved A/B campaigns of builds and flag
bundles on temporary gateways and clients. It refuses to start beside a
production service and tears everything down. `scripts/ab_analyze.py`
applies the 0.90 block-contrast gate, and `scripts/dc_gate.py` judges the
`dc-long-haul` gate against frozen bounds. See docs/VALIDATING-A-BUNDLE.md.
