CREATE INDEX votes_post_id_idx on votes USING hash (PostId)
       WITH (FILLFACTOR = 100);
CREATE INDEX votes_type_idx on votes USING btree (VoteTypeId)
       WITH (FILLFACTOR = 100);
CREATE INDEX votes_creation_date_idx on votes USING btree (CreationDate)
       WITH (FILLFACTOR = 100);

