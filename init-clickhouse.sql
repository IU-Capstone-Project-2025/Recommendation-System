/*
 BOOK RECOMMENDATION SYSTEM SCHEMA FOR CLICKHOUSE
 
 This schema implements a complete book recommendation system with:
 - Core entity tables (Users, Books, Genres, Tags, Types)
 - User engagement tracking (ratings, reading status)
 - Relationship mapping tables
 - Analytical views for recommendation calculations
 - Materialized views for performance optimization
 
 All tables use ReplacingMergeTree engine for efficient updates and deduplication.
 The system includes automatic data expiration (TTL) for recommendation tables.
 */
-- ====================== CORE ENTITY TABLES ======================
CREATE TABLE IF NOT EXISTS User (
    id UInt32,
    username String,
    updatets DateTime DEFAULT now(),
    -- Timestamp of last update
    PRIMARY KEY (id) -- Primary key for user identification
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (id);

-- Physical sort order for optimal queries
CREATE TABLE IF NOT EXISTS Book (
    id UInt32,
    title String,
    author String,
    year Nullable(Int32),
    -- Publication year (optional)
    imgurl Nullable(String),
    -- Book cover image URL (optional)
    description Nullable(String),
    -- Detailed description (optional)
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS Genre (
    id UInt32,
    name String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS Tag (
    id UInt32,
    name String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS TYPE (
    id UInt32,
    name String,
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (id);

-- ====================== USER ENGAGEMENT TABLES ======================
CREATE TABLE IF NOT EXISTS Score (
    userid UInt32,
    bookid UInt32,
    score Int32,
    -- User rating (e.g., 1-5 stars)
    isactual UInt8 DEFAULT 1,
    -- 1 = active rating, 0 = historical
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (userid, bookid);

-- Optimized for user-book queries
CREATE TABLE IF NOT EXISTS Planned (
    userid UInt32,
    bookid UInt32,
    isactual UInt8 DEFAULT 1,
    -- Tracks current reading plans
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (userid, bookid);

CREATE TABLE IF NOT EXISTS Reading (
    userid UInt32,
    bookid UInt32,
    isactual UInt8 DEFAULT 1,
    -- Current reading status
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (userid, bookid);

CREATE TABLE IF NOT EXISTS Completed (
    userid UInt32,
    bookid UInt32,
    isactual UInt8 DEFAULT 1,
    -- Tracks completed reads
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (userid, bookid);

CREATE TABLE IF NOT EXISTS Message (
    id UInt32,
    userid UInt32,
    bookid UInt32,
    message String,
    isactual UInt8 DEFAULT 1,
    -- Message visibility flag
    updatets DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (id);

-- ====================== RELATIONSHIP MAPPING TABLES ======================
CREATE TABLE IF NOT EXISTS BookGenre (
    bookid UInt32,
    genreid UInt32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (bookid, genreid);

-- Many-to-many relationship
CREATE TABLE IF NOT EXISTS BookTag (
    bookid UInt32,
    tagid UInt32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (bookid, tagid);

CREATE TABLE IF NOT EXISTS BookType (
    bookid UInt32,
    typeid UInt32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (bookid, typeid);

-- ====================== ANALYTICAL VIEWS ======================
/*
 TYPE SCORE VIEW
 Calculates normalized user preference scores for book types
 - Scores are normalized against each user's maximum values
 - Considers both average rating and number of votes
 */

CREATE VIEW IF NOT EXISTS TypeScore AS WITH scores AS (
    SELECT
        ut.userid AS userid,
        ut.typeid AS typeid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        User u
        INNER JOIN Score s ON u.id = s.userid
        INNER JOIN BookType bt ON s.bookid = bt.bookid
        RIGHT JOIN (
            SELECT
                u.id AS userid,
                t.id AS typeid
            FROM
                User u
                CROSS JOIN TYPE t
        ) ut ON u.id = ut.userid
        AND bt.typeid = ut.typeid
    WHERE
        s.isactual = 1
    GROUP BY
        ut.userid,
        ut.typeid
),
user_maxima AS (
    SELECT
        userid,
        MAX(score) AS max_score,
        MAX(votes) AS max_votes
    FROM
        scores
    GROUP BY
        userid
)
SELECT
    s.userid,
    s.typeid,
    IF(max_score > 0, score / max_score, 0) AS score,
    IF(max_votes > 0, votes / max_votes, 0) AS votes
FROM
    scores s
    JOIN user_maxima um ON s.userid = um.userid;

CREATE VIEW IF NOT EXISTS GenreScore AS WITH scores AS (
    SELECT
        ut.userid AS userid,
        ut.genreid AS genreid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        User u
        INNER JOIN Score s ON u.id = s.userid
        INNER JOIN BookGenre bg ON s.bookid = bg.bookid
        RIGHT JOIN (
            SELECT
                u.id AS userid,
                g.id AS genreid
            FROM
                User u
                CROSS JOIN Genre g
        ) ut ON u.id = ut.userid
        AND bg.genreid = ut.genreid
    WHERE
        s.isactual = 1
    GROUP BY
        ut.userid,
        ut.genreid
),
user_maxima AS (
    SELECT
        userid,
        MAX(score) AS max_score,
        MAX(votes) AS max_votes
    FROM
        scores
    GROUP BY
        userid
)
SELECT
    s.userid,
    s.genreid,
    IF(max_score > 0, score / max_score, 0) AS score,
    IF(max_votes > 0, votes / max_votes, 0) AS votes
FROM
    scores s
    JOIN user_maxima um ON s.userid = um.userid;

CREATE VIEW IF NOT EXISTS TagScore AS WITH scores AS (
    SELECT
        ut.userid AS userid,
        ut.tagid AS tagid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        User u
        INNER JOIN Score s ON u.id = s.userid
        INNER JOIN BookTag bt ON s.bookid = bt.bookid
        RIGHT JOIN (
            SELECT
                u.id AS userid,
                t.id AS tagid
            FROM
                User u
                CROSS JOIN Tag t
        ) ut ON u.id = ut.userid
        AND bt.tagid = ut.tagid
    WHERE
        s.isactual = 1
    GROUP BY
        ut.userid,
        ut.tagid
),
user_maxima AS (
    SELECT
        userid,
        MAX(score) AS max_score,
        MAX(votes) AS max_votes
    FROM
        scores
    GROUP BY
        userid
)
SELECT
    s.userid,
    s.tagid,
    IF(max_score > 0, score / max_score, 0) AS score,
    IF(max_votes > 0, votes / max_votes, 0) AS votes
FROM
    scores s
    JOIN user_maxima um ON s.userid = um.userid;

-- ====================== MATERIALIZED VIEWS ======================
/*
 PERSONALIZED RECOMMENDATION ENGINE
 Calculates compatibility scores between users and books based on:
 - Type preferences (33% weight)
 - Genre preferences (33% weight)
 - Tag preferences (33% weight)
 */
CREATE TABLE IF NOT EXISTS PersonalPart (
    userid UInt32,
    bookid UInt32,
    compatibility Float32,
    -- 0-1 compatibility score
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (userid, bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS PersonalPart_MV TO PersonalPart AS
SELECT
    u.id AS userid,
    b.id AS bookid,
    /* Combined compatibility score calculation */
    (
        COALESCE(AVG(ts.score), 0) * COALESCE(AVG(ts.votes), 0) + COALESCE(AVG(gs.score), 0) * COALESCE(AVG(gs.votes), 0) + COALESCE(AVG(tgs.score), 0) * COALESCE(AVG(tgs.votes), 0)
    ) / 3 AS compatibility,
    now() AS updatets
FROM
    User u
    CROSS JOIN Book b -- Consider all possible user-book combinations
    LEFT JOIN BookType bt ON b.id = bt.bookid
    LEFT JOIN BookGenre bg ON b.id = bg.bookid
    LEFT JOIN BookTag btg ON b.id = btg.bookid
    LEFT JOIN TypeScore ts ON u.id = ts.userid
    AND bt.typeid = ts.typeid
    LEFT JOIN GenreScore gs ON u.id = gs.userid
    AND bg.genreid = gs.genreid
    LEFT JOIN TagScore tgs ON u.id = tgs.userid
    AND btg.tagid = tgs.tagid
GROUP BY
    u.id,
    b.id;

CREATE TABLE IF NOT EXISTS Top (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS Top_MV TO Top AS WITH scores AS (
    SELECT
        b.id AS bookid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        Book b
        LEFT JOIN Score s ON b.id = s.bookid
        AND s.isactual = TRUE
    GROUP BY
        b.id
),
normalized AS (
    SELECT
        s.bookid,
        COALESCE(
            s.score / NULLIF(
                (
                    SELECT
                        MAX(s.score) AS score
                    FROM
                        scores s
                ),
                0
            ),
            0
        ) AS normscore,
        COALESCE(
            s.votes :: FLOAT / NULLIF(
                (
                    SELECT
                        MAX(s.votes) AS votes
                    FROM
                        scores s
                ),
                0
            ),
            0
        ) AS normvotes
    FROM
        scores s
)
SELECT
    n.bookid AS bookid,
    ROW_NUMBER() OVER (
        ORDER BY
            n.normscore * n.normvotes DESC
    ) AS rank,
    n.normscore * n.normvotes AS compscore,
    now() AS updatets
FROM
    normalized n;

CREATE TABLE IF NOT EXISTS WeeklyTop (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS WeeklyTop_MV TO WeeklyTop AS WITH weekscores AS (
    SELECT
        b.id AS bookid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        Book b
        LEFT JOIN Score s ON b.id = s.bookid
    WHERE
        s.isactual = TRUE
        AND s.updatets >= NOW() - INTERVAL '7' DAY
    GROUP BY
        b.id
),
normalized AS (
    SELECT
        s.bookid,
        COALESCE(
            s.score / NULLIF(
                (
                    SELECT
                        MAX(s.score) AS score
                    FROM
                        weekscores s
                ),
                0
            ),
            0
        ) AS normscore,
        COALESCE(
            s.votes :: FLOAT / NULLIF(
                (
                    SELECT
                        MAX(s.votes) AS votes
                    FROM
                        weekscores s
                ),
                0
            ),
            0
        ) AS normvotes
    FROM
        weekscores s
)
SELECT
    n.bookid AS bookid,
    ROW_NUMBER() OVER (
        ORDER BY
            n.normscore * n.normvotes DESC
    ) AS rank,
    n.normscore * n.normvotes AS compscore,
    now() AS updatets
FROM
    normalized n;

CREATE TABLE IF NOT EXISTS Recommendations (
    userid UInt32,
    bookid UInt32,
    rank UInt32,
    compatibility Float32,
    updatets DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updatets)
ORDER BY
    (userid, bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS Recommendations_MV TO Recommendations AS
SELECT
    pp.userid,
    pp.bookid,
    ROW_NUMBER() OVER (
        PARTITION BY pp.userid
        ORDER BY
            COALESCE(pp.compatibility, 0) + COALESCE(t.compscore, 0) DESC
    ) AS rank,
    (
        COALESCE(pp.compatibility, 0) + COALESCE(t.compscore, 0)
    ) AS compatibility,
    now() AS updatets
FROM
    PersonalPart pp
    INNER JOIN Top t ON t.bookid = pp.bookid
WHERE
    (pp.userid, pp.bookid) NOT IN (
        SELECT
            userid,
            bookid
        FROM
            Completed
        WHERE
            isactual = TRUE
    );

-- ====================== DATA RETENTION POLICIES ======================
/* 
 TTL (Time-To-Live) SETTINGS
 Automatically expire old recommendation data to:
 - Save storage space
 - Ensure recommendations stay current
 - Maintain system performance
 */
ALTER TABLE
    PersonalPart
MODIFY
    TTL updatets + INTERVAL 10 MINUTE;

ALTER TABLE
    Top
MODIFY
    TTL updatets + INTERVAL 10 MINUTE;

ALTER TABLE
    WeeklyTop
MODIFY
    TTL updatets + INTERVAL 10 MINUTE;

ALTER TABLE
    Recommendations
MODIFY
    TTL updatets + INTERVAL 10 MINUTE;