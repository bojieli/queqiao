A flow that ends at its own idle or lifetime limit now tells the other end
with a RESET, whichever end it is. A client's idle timeout used to reach the
gateway only as its streams closing, which the gateway took for a path
failure: the destination was held open for up to 45 seconds waiting for a
replacement lane, long after the application had seen the connection end.
