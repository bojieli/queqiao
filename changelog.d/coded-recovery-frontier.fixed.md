Limit reliable-stream recovery to the bytes pending when a coded send
stalls. Later interactive exchanges keep coding through delayed
acknowledgements, and dispatched stream retries remain reliable after
their recovery is acknowledged.
