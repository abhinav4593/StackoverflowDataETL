CREATE INDEX user_acc_id_idx ON users USING hash (Id)
       WITH (FILLFACTOR = 100);
CREATE INDEX user_display_idx ON users USING hash (DisplayName)
       WITH (FILLFACTOR = 100);
CREATE INDEX user_up_votes_idx ON users USING btree (UpVotes)
       WITH (FILLFACTOR = 100);
CREATE INDEX user_down_votes_idx ON users USING btree (DownVotes)
       WITH (FILLFACTOR = 100);
CREATE INDEX user_created_at_idx ON users USING btree (CreationDate)
       WITH (FILLFACTOR = 100);
