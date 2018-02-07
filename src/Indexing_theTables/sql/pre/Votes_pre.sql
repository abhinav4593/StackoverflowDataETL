CREATE TABLE IF NOT EXISTS Votes (
   Id                int         not NULL    ,
   PostId            int         not NULL    ,
   VoteTypeId        int         not NULL    ,
   UserId            int                     ,
   CreationDate      timestamp   not NULL    ,
   BountyAmount      int
);

