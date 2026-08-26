The datacenter profile's flow classification is validated on the live path.
Sixteen concurrent 300KB requests are demoted to bulk on the access-link
thresholds and none on the datacenter ones, which is what those thresholds
exist to change: a few hundred kilobytes is one ordinary request on a
datacenter leg and a transfer on an access link, and a demoted flow stops
preferring the coding that repairs a gap inside the round trip carrying it.
