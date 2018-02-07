CREATE TABLE IF NOT EXISTS Users (
   Id                int         not NULL	 ,
   Reputation        int         not NULL    ,
   CreationDate      timestamp   not NULL    ,
   DisplayName       varchar(40) not NULL    ,
   LastAccessDate    timestamp               ,
   WebsiteUrl        TEXT                    ,
   Location          TEXT                    ,
   AboutMe           TEXT                    ,
   Views             int         not NULL    ,
   UpVotes           int         not NULL    ,
   DownVotes         int         not NULL    ,
   ProfileImageUrl   text                    ,
   Age               int                     ,
   AccountId         int         -- NULL accountId == deleted account?
);

