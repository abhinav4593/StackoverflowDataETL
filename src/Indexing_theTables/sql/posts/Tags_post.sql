CREATE INDEX tags_count_idx on Tags USING btree (tag_count)
       WITH (FILLFACTOR = 100);
CREATE INDEX tags_name_idx on Tags USING hash ("tag_Name")
       WITH (FILLFACTOR = 100);
