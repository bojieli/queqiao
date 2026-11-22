Bound destination TCP dialing to two staggered candidates and share the open
deadline across resolved addresses. A blackholed first address no longer
exhausts the entire deadline while a later address is reachable. All
candidates remain subject to the existing public-address policy, and losing
sockets close without carrying application data.
