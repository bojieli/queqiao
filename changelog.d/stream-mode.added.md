`pathmeasure -mode stream` measures a token stream, which none of the other
modes describes: it reports time to first token and how far behind the
generator's own schedule each token arrived, rather than a total the reader
never waits for in one piece. `-mode streamserve` emits tokens at a fixed
cadence so the generator is not a variable in the comparison.
