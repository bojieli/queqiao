`scripts/carrier_shadow_summary.py` handles four attribution edge cases
before stage-8 calibration. Outage starts that share a timestamp are taken
in `outage_id` order on both endpoints, so an asynchronously logged newer
start no longer closes the wrong outage. A state transition that names a
newer transport epoch now ends an earlier incident. An outage end whose
start is missing from the capture no longer closes an outage with a
different identifier. A reused process/carrier key with a lower epoch no
longer makes progress lookups quadratic.
