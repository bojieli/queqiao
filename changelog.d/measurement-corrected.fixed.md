The ASR and TTS figures in the datacenter characterization were medians across
eight audio files of 146KB to 405KB, labelled with the size of whichever
request ran last. The mislabelling had a consequence rather than being
cosmetic: a tuned client was recorded at 225.8ms for what was called a 355KB
upload, which is below the floor that size has on a 200ms path with a 30ms
model. Re-measured with one fixed file, and the section is marked superseded
rather than deleted.
