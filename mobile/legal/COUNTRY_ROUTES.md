# Bundled country route set

`mobile/ios/PacketTunnel/Resources/cn-direct.bin` is a packed list of the
address blocks the regional registry records as delegated to China. The iOS
tunnel can be asked to keep those blocks off the tunnel; the feature is
labelled experimental in the app and the reasons are below.

## Source

| | |
|---|---|
| File | `delegated-apnic-latest` |
| URL | <https://ftp.apnic.net/stats/apnic/delegated-apnic-latest> |
| Publisher | Asia-Pacific Network Information Centre (APNIC) |
| Serial | `88964`, dated `20260829` (the file's `2\|apnic\|…` header line) |
| SHA-256 of the source | `a8c0f001e70b5c32221fc5cc37760bb63f4eef058959124eb29e62bc66a6dd44` |
| SHA-256 of `cn-direct.bin` | `900983e9e5993ce1033c2734bde2914563194f89e70c799a75031f46bbfcc896` |
| Contents | 5493 IPv4 and 2014 IPv6 blocks, 61719 bytes, from 10833 delegation records |

## Terms

APNIC publishes the delegated-* statistics files without a software licence,
under a conditions-of-use notice carried in the file's own header:

> The files are freely available for download and use on the condition that
> APNIC will not be held responsible for any loss or damage arising from the
> use of the information contained in these reports.
>
> APNIC endeavours to the best of its ability to ensure the accuracy of these
> reports; however, APNIC makes no guarantee in this regard.
>
> In particular, it should be noted that these reports seek to indicate where
> resources were first allocated or assigned. It is not intended that these
> reports be considered as an authoritative statement of the location in which
> any specific resource may currently be in use.

That third paragraph is the reason the feature ships marked experimental. The
set says where a block was *delegated*, which is not where it is *used*: an
allocation to a Chinese holder may serve traffic anywhere, and a service used
from China may sit in space delegated elsewhere.

## Regenerating

```
curl -O https://ftp.apnic.net/stats/apnic/delegated-apnic-latest
scripts/generate_cn_geoip.py \
    --input delegated-apnic-latest \
    --output mobile/ios/PacketTunnel/Resources/cn-direct.bin
```

The script prints the block counts, the byte size, and — when the set has to be
aggregated to fit the route cap — how many addresses that aggregation
over-includes. Today it fits exactly and aggregates nothing, so no address
leaves the tunnel that the registry does not name.

Record the new serial, both digests, and the counts in the table above when the
resource is refreshed. `scripts/test_cn_geoip.py` covers the generator and
`QueqiaoTests/CountryRoutesTests.swift` decodes the committed artifact, so a
malformed or truncated regeneration fails the build rather than the device.

## Format

A 16-byte header — `QQGO`, a format version byte, two reserved bytes, then the
IPv4 and IPv6 block counts as big-endian `u32` — followed by 5 bytes per IPv4
block (address, prefix length) and 17 per IPv6 block. Blocks are sorted and
already collapsed, because the parse runs inside a NetworkExtension against the
fixed memory profile in `docs/MOBILE-MEMORY.md` and should do no work the build
could have done. `Shared/CountryRoutes.swift` reads it; `decode()` in the
generator is the other half of the contract.
