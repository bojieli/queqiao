The bundled China preset keeps Chinese services direct by name, not only by
address, and the registry set is refreshed to APNIC serial 88964. The address
set records where a block was delegated, which is not where it is used, so a
Chinese service answering from a CDN in space delegated elsewhere never
matched GEOIP,CN and took the tunnel while the toggle said direct: ip138.com
answers 138.113.x and bilibili.com 148.153.x, and a Chinese resolver returns
those same addresses, so nothing was being mis-resolved and no fresher list
could have helped. The preset now names the major Chinese services on TLDs
that a .cn rule cannot reach, together with the CDN and asset domains they
load from, and keeps GEOIP,CN last so the names are reached first. It is a
starting point rather than coverage: a geosite-derived list pasted into the
rule editor is still the way to be thorough. Refreshing the registry set moved
three blocks in ten days, which is the measure of how little staleness was
ever the problem.
