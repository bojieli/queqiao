Every flow completion record now reports the DATA payload this endpoint sent
by class (new, interactive, bulk) and substrate (coded, stream), whether the
flow was ever BULK, and how often it left and re-entered BULK. The metrics
endpoint exports the same split per endpoint as
`queqiao_data_class_bytes_total{class,substrate}`, and class changes are
logged at debug level with the flow's age. These are what an evaluation reads
to see which bytes a classifier decision actually affected, such as the share
of an LLM response sent interactive.
