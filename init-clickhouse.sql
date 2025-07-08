CREATE TABLE IF NOT EXISTS User (
    id UInt32,
    username String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS Book (
    id UInt32,
    title String,
    author String,
    year Nullable(Int32),
    imgurl Nullable(String),
    description Nullable(String),
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS Genre (
    id UInt32,
    name String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS Tag (
    id UInt32,
    name String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS Type (
    id UInt32,
    name String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree()
ORDER BY (id);

-- Junction tables
CREATE TABLE IF NOT EXISTS Score (
    userid UInt32,
    bookid UInt32,
    score Int32,
    isactual UInt8 DEFAULT 1,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (userid, bookid);

CREATE TABLE IF NOT EXISTS Planned (
    userid UInt32,
    bookid UInt32,
    isactual UInt8 DEFAULT 1,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (userid, bookid);

CREATE TABLE IF NOT EXISTS Reading (
    userid UInt32,
    bookid UInt32,
    isactual UInt8 DEFAULT 1,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (userid, bookid);

CREATE TABLE IF NOT EXISTS Completed (
    userid UInt32,
    bookid UInt32,
    isactual UInt8 DEFAULT 1,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (userid, bookid);

CREATE TABLE IF NOT EXISTS Message (
    id UInt32,
    userid UInt32,
    bookid UInt32,
    message String,
    isactual UInt8 DEFAULT 1,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree()
ORDER BY (id);

CREATE TABLE IF NOT EXISTS BookGenre (
    bookid UInt32,
    genreid UInt32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (bookid, genreid);

CREATE TABLE IF NOT EXISTS BookTag (
    bookid UInt32,
    tagid UInt32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (bookid, tagid);

CREATE TABLE IF NOT EXISTS BookType (
    bookid UInt32,
    typeid UInt32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (bookid, typeid);

-- Views (ClickHouse doesn't support materialized views in the same way as Greenplum)
CREATE VIEW IF NOT EXISTS TypeScore AS
WITH scores AS (
    SELECT 
        ut.userid as userid,
        ut.typeid as typeid,
        avg(s.score) as score,
        count(s.score) as votes
    FROM User u
    INNER JOIN Score s ON u.id = s.userid
    INNER JOIN BookType bt ON s.bookid = bt.bookid
    RIGHT JOIN (SELECT u.id as userid, t.id as typeid FROM User u CROSS JOIN Type t) ut ON u.id = ut.userid AND bt.typeid = ut.typeid
    WHERE s.isactual = 1
    GROUP BY ut.userid, ut.typeid 
),
user_maxima AS (
    SELECT 
        userid,
        max(score) as max_score,
        max(votes) as max_votes
    FROM scores
    GROUP BY userid
)
SELECT
    s.userid,
    s.typeid,
    if(max_score > 0, score / max_score, 0) as score,
    if(max_votes > 0, votes / max_votes, 0) as votes
FROM scores s
JOIN user_maxima um ON s.userid = um.userid;

CREATE VIEW IF NOT EXISTS GenreScore AS
WITH scores AS (
    SELECT 
        ut.userid as userid,
        ut.genreid as genreid,
        avg(s.score) as score,
        count(s.score) as votes
    FROM User u
    INNER JOIN Score s ON u.id = s.userid
    INNER JOIN BookGenre bg ON s.bookid = bg.bookid
    RIGHT JOIN (SELECT u.id as userid, g.id as genreid FROM User u CROSS JOIN Genre g) ut ON u.id = ut.userid AND bg.genreid = ut.genreid
    WHERE s.isactual = 1
    GROUP BY ut.userid, ut.genreid
),
user_maxima AS (
    SELECT 
        userid,
        max(score) as max_score,
        max(votes) as max_votes
    FROM scores
    GROUP BY userid
)
SELECT
    s.userid,
    s.genreid,
    if(max_score > 0, score / max_score, 0) as score,
    if(max_votes > 0, votes / max_votes, 0) as votes
FROM scores s
JOIN user_maxima um ON s.userid = um.userid;

CREATE VIEW IF NOT EXISTS TagScore AS
WITH scores AS (
    SELECT 
        ut.userid as userid,
        ut.tagid as tagid,
        avg(s.score) as score,
        count(s.score) as votes
    FROM User u
    INNER JOIN Score s ON u.id = s.userid
    INNER JOIN BookTag bt ON s.bookid = bt.bookid
    RIGHT JOIN (SELECT u.id as userid, t.id as tagid FROM User u CROSS JOIN Tag t) ut ON u.id = ut.userid AND bt.tagid = ut.tagid
    WHERE s.isactual = 1
    GROUP BY ut.userid, ut.tagid
),
user_maxima AS (
    SELECT 
        userid,
        max(score) as max_score,
        max(votes) as max_votes
    FROM scores
    GROUP BY userid
)
SELECT
    s.userid,
    s.tagid,
    if(max_score > 0, score / max_score, 0) as score,
    if(max_votes > 0, votes / max_votes, 0) as votes
FROM scores s
JOIN user_maxima um ON s.userid = um.userid;

-- For materialized views, ClickHouse has a different approach using MATERIALIZED VIEW
CREATE TABLE IF NOT EXISTS PersonalPart (
    userid UInt32,
    bookid UInt32,
    compatibility Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY (userid, bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS PersonalPart_MV TO PersonalPart AS
SELECT 
    u.id as userid,
    b.id as bookid,
    (coalesce(avg(ts.score),0)*coalesce(avg(ts.votes),0) + 
     coalesce(avg(gs.score),0)*coalesce(avg(gs.votes),0) + 
     coalesce(avg(tgs.score),0)*coalesce(avg(tgs.votes),0)) / 3 as compatibility,
    now() as updatets 
FROM User u
CROSS JOIN Book b
LEFT JOIN BookType bt ON b.id = bt.bookid
LEFT JOIN BookGenre bg ON b.id = bg.bookid
LEFT JOIN BookTag btg ON b.id = btg.bookid
LEFT JOIN TypeScore ts ON u.id = ts.userid AND bt.typeid = ts.typeid
LEFT JOIN GenreScore gs ON u.id = gs.userid AND bg.genreid = gs.genreid
LEFT JOIN TagScore tgs ON u.id = tgs.userid AND btg.tagid = tgs.tagid
GROUP BY u.id, b.id;

CREATE TABLE IF NOT EXISTS Top (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY (bookid, rank);

CREATE MATERIALIZED VIEW IF NOT EXISTS Top_MV TO Top AS
WITH scores AS (
    SELECT 
        b.id as bookid,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM Book b
    LEFT JOIN Score s ON b.id = s.bookid AND s.isactual = TRUE
    GROUP BY b.id
),
normalized AS (
    SELECT
        s.bookid,
        COALESCE(s.score / NULLIF((SELECT MAX(s.score) as score FROM scores s), 0), 0) as normscore,
        COALESCE(s.votes::float / NULLIF((SELECT MAX(s.votes) as votes FROM scores s), 0), 0) as normvotes
    FROM scores s
)
SELECT 
    n.bookid as bookid,
    ROW_NUMBER() OVER (ORDER BY n.normscore * n.normvotes DESC) as rank,
    n.normscore * n.normvotes as compscore,
    now() AS updatets
FROM normalized n;

CREATE TABLE IF NOT EXISTS WeeklyTop (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY (bookid, rank);

CREATE MATERIALIZED VIEW IF NOT EXISTS WeeklyTop_MV TO WeeklyTop AS
WITH weekscores AS (
    SELECT 
        b.id as bookid,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM Book b
    LEFT JOIN Score s ON b.id = s.bookid
    WHERE s.isactual = TRUE AND s.updatets >= NOW() - INTERVAL '7' DAY
    GROUP BY b.id
),
normalized AS (
    SELECT
        s.bookid,
        COALESCE(s.score / NULLIF((SELECT MAX(s.score) as score FROM weekscores s), 0), 0) as normscore,
        COALESCE(s.votes::float / NULLIF((SELECT MAX(s.votes) as votes FROM weekscores s), 0), 0) as normvotes
    FROM weekscores s
)
SELECT 
    n.bookid as bookid,
    ROW_NUMBER() OVER (ORDER BY n.normscore * n.normvotes DESC) as rank,
    n.normscore * n.normvotes as compscore,
    now() AS updatets
FROM normalized n;

CREATE TABLE IF NOT EXISTS Recommendations (
    userid UInt32,
    bookid UInt32,
    rank UInt32,
    compatibility Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY (userid, bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS Recommendations_MV TO Recommendations AS
SELECT 
    pp.userid,
    pp.bookid,
    ROW_NUMBER() OVER (PARTITION BY pp.userid ORDER BY COALESCE(pp.compatibility,0) + COALESCE(t.compscore,0) DESC) as rank,
    (COALESCE(pp.compatibility,0) + COALESCE(t.compscore,0)) as compatibility,
    now() AS updatets
FROM PersonalPart pp
INNER JOIN Top t ON t.bookid = pp.bookid
WHERE (pp.userid, pp.bookid) NOT IN (SELECT userid, bookid FROM Completed WHERE isactual = TRUE);