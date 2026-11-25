The flow classifier's recent-traffic window now ages by the time that has
actually passed. It used to roll over once however long a flow had been idle,
so the first write after any idle period still counted the burst before it as
recent. A response to a 120-128 KiB request, or the first message after a
quiet spell, could then make the flow BULK, and under the legacy classifier
BULK is permanent.
