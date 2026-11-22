Keep receive payloads charged while application writes block, and deliver
reordered chunks without allocating a second window-sized buffer. Field
validation now rejects truncated HTTP bodies and can enforce a known body
digest; absent resource measurements are no longer reported as a resource-
convergence pass.
