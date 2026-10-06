Keep the published close sequence immutable when source EOF races a full
application close, and treat abort acknowledgements as cancellation rather
than delivery credit.
