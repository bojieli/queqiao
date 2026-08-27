The tunnel's metrics report what routing is doing: how many rules loaded, and
how many flows were proxied, sent direct, refused, decided by name, or refused
because they named a handle the resolver had forgotten. Zero rules with
traffic flowing is a different fault from a loaded list whose direct count
never moves, and an operator could not tell them apart without both numbers.
