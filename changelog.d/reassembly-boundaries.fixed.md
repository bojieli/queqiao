Discard already-delivered duplicate data without consuming receive-window
memory, reject partial cursor overlaps and conflicting final offsets before
mutating receive state, and preserve explicit budgets when defaulting limits.
