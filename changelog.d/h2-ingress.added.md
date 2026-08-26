`pathmeasure -mode h2proxy` terminates HTTP/2 with large windows and streams
the body onward, which makes a small receive window irrelevant without
changing the receiver.
