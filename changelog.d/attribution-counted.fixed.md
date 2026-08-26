A class declared from flow attribution is counted as a class transition. It
was not, because a declared class never passes through the classifier's
observation path, so the whole mechanism was invisible in telemetry: an
operator reading `queqiao_class_transitions` saw zero and could not tell
hints that never fired from hints that were never configured.
