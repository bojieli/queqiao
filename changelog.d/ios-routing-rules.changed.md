iOS routing is one setting again, and all of it can be changed while the
tunnel is connected. The traffic policy, the bundled China toggle, the typed
bypass list, and the rule list were four controls answering one question, and
the first three did not agree about who was in charge: the bypass list and the
country set applied whatever the policy said, so a profile reading "All
traffic" could be keeping whole countries off the tunnel. There is now a
routing mode, the bypass rules underneath it, and the rule list, all carried
in one value the settings screen and the tunnel both compose through.
Switching to global routing leaves the rules stored rather than making someone
dismantle a list to turn the tunnel up. None of it needs a reconnect: the app
saves the change and asks the running provider to re-read it, and the provider
reinstalls the routes and hands the new rule list to the core without
restarting the packet engine. Flows already open keep the rules they started
under, which is the cost of not dropping the connection to edit them. A
catalog written before the merge migrates on load, and anything that was
carving traffic out of the tunnel comes back as a bypass rule rather than
quietly rejoining it.
