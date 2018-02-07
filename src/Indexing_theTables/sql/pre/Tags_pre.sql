CREATE TABLE IF NOT EXISTS Tags (
    Id                    int  not NULL    ,
    TagName               text not NULL    ,
    Count                 int,
    ExcerptPostId         int,
    WikiPostId            int
);
