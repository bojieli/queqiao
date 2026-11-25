`--classifier phased` lets a flow that has carried a bulk transfer behave as
an interactive one again, on clients and gateways. Under the default,
`--classifier legacy`, BULK is permanent: an SSH session after one large
output, a browser's HTTP/2 connection after a page, an LLM call on a reused
connection or with a large prompt, and a messaging connection after a photo
send every later small write as uncoded BULK data. Under `phased` the sticky
class still decides lane choice and bulk isolation, but the DATA class, lane
queue, interactive budget flag and coding follow what the flow does now: a
write of at most two packets onto an empty send queue is sent interactive and
coded, and the flow leaves BULK after two payload-carrying one-second windows
with no backlog and no high rate, re-entering it only after a backlog held for
a second (or four round trips). A backlogged transfer stays BULK throughout.
Each endpoint classifies the direction it sends, so the gateway must also run
`phased` for downstream data. `phased` is refused with the `dc-long-haul`
profile.
