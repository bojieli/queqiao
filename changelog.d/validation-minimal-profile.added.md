The live validation harness has a minimal default profile: four trial types
that mix bulk transfers, LLM streaming, terminal keystrokes, a video call
over UDP, a page load and idle connections that wake. It stops each
campaign as soon as its gates are decided, with a corrected stopping rule,
and its dry run prints the expected wall time, about two and a half to
three hours per path.
