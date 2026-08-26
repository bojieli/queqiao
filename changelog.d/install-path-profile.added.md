`deploy/install-client.sh --path-profile` and `queqiaod service install
--path-profile` carry a path profile into the installed service. The flag
existed on `queqiaod client` and nothing between the installer and the unit
file carried it, so running the datacenter profile as a service meant editing
a definition the installer rewrites on every upgrade. An unknown name fails
the install rather than starting a service with policy nobody asked for.
