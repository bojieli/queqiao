`--class-hint` declares the class a flow starts in from what produced it, as
a repeatable `<match>=<interactive|bulk>`. It is refused without `--flow-
metadata-socket`, since nothing would be there to ask, and a misspelled
class name fails at startup rather than matching nothing at runtime.
