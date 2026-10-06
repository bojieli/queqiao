Serialize UDP relay writes with their shared socket deadlines and cancel
queued or expired writes. Count payload frames only after successful writes
on the substrate actually used, including coded-to-stream fallback.
