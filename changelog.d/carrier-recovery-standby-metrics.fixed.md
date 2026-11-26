Under `--recovery=carrier`, a standby dial to a gateway with several
addresses races only as many address candidates as the device has free
connection slots, so on iOS it dials them one at a time instead of going
over the two-connection limit. The carrier manager's counters are exported
as Prometheus metrics: `queqiao_carrier_suspects_total`,
`queqiao_carrier_promotions_total`, `queqiao_carrier_standby_dials_total`,
`queqiao_carrier_standby_failures_total`,
`queqiao_carrier_replacement_dials_total`,
`queqiao_carrier_rebinds_total{carrier="same"|"new"}`,
`queqiao_carrier_binding_deadline_failures_total` and
`queqiao_carrier_tcp_handoffs_total`.
