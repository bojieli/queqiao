The datacenter deployment guide records the capture path for applications
that cannot be reconfigured, measured end to end on the China-US path: an
unmodified application reaches 624.9ms cold on a 300KB request against
1089.3ms direct, and 381.2ms once warm, with the connect leg falling from
187.3ms to 0.2ms.
