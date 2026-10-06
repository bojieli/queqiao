Reuse authenticated responses on newly opened pooled streams as bounded path
proof, avoiding redundant probe round trips during repeated short flows while
retaining stale-connection validation. Buffered replies cannot refresh proof
beyond the stream's creation or undo a later transport failure.
