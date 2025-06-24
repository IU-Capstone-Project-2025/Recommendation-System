CREATE TABLE "User" (
    ID SERIAL,
    username VARCHAR(100) NOT NULL,
    codedpass VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID))
DISTRIBUTED BY (ID);

CREATE TABLE Book (
    ID SERIAL,
    title VARCHAR(100) NOT NULL,
    author VARCHAR(100) NOT NULL,
    year INT,
    score FLOAT,
    votes INT,
    imgurl VARCHAR(1000),
    description TEXT,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID))
DISTRIBUTED BY (ID);

CREATE TABLE Genre (
    ID SERIAL,
    name VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID))
DISTRIBUTED BY (ID);

CREATE TABLE Tag (
    ID SERIAL,
    name VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID))
DISTRIBUTED BY (ID);

CREATE TABLE Type (
    ID SERIAL,
    name VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID))
DISTRIBUTED BY (ID);

-- Junction tables with composite distribution
CREATE TABLE Score (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    score INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID))
DISTRIBUTED BY (userID, bookID);

CREATE TABLE Planned (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID))
DISTRIBUTED BY (userID, bookID);

CREATE TABLE Reading (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID))
DISTRIBUTED BY (userID, bookID);

CREATE TABLE Completed (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID))
DISTRIBUTED BY (userID, bookID);

CREATE TABLE Message (
    ID SERIAL,
    userID INT NOT NULL,
    bookID INT NOT NULL,
    message TEXT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID))
DISTRIBUTED BY (ID);

CREATE TABLE BookGenre (
    bookID INT NOT NULL,
    genreID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, genreID),
    FOREIGN KEY (bookID) REFERENCES Book(ID),
    FOREIGN KEY (genreID) REFERENCES Genre(ID))
DISTRIBUTED BY (bookID, genreID);

CREATE TABLE BookTag (
    bookID INT NOT NULL,
    tagID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, tagID),
    FOREIGN KEY (bookID) REFERENCES Book(ID),
    FOREIGN KEY (tagID) REFERENCES Tag(ID))
DISTRIBUTED BY (bookID, tagID);

CREATE TABLE BookType (
    bookID INT NOT NULL,
    typeID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, typeID),
    FOREIGN KEY (bookID) REFERENCES Book(ID),
    FOREIGN KEY (typeID) REFERENCES Type(ID))
DISTRIBUTED BY (bookID, typeID);

-- Regular View
CREATE VIEW TypeScore AS
WITH scores AS (
    SELECT 
        ut.userID as userID,
        ut.typeID as typeID,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM "User" u
    INNER JOIN Score s ON u.ID = s.userID
    INNER JOIN BookType bt ON s.bookID = bt.bookID
    RIGHT JOIN (SELECT u.ID as userID, t.ID as typeID FROM "User" u CROSS JOIN Type t) ut ON u.ID = ut.userID AND bt.typeID = ut.typeID
    WHERE s.isactual = TRUE
    GROUP BY ut.userID, ut.typeID 
),
user_maxima AS (
    SELECT 
        userID,
        MAX(score) as max_score,
        MAX(votes) as max_votes
    FROM scores
    GROUP BY userID
)
SELECT
    s.userID,
    s.typeID,
    CASE 
        WHEN um.max_score > 0 THEN s.score / um.max_score 
        ELSE 0 
    END as score,
    CASE 
        WHEN um.max_votes > 0 THEN s.votes::float / um.max_votes 
        ELSE 0 
    END as votes
FROM scores s
JOIN user_maxima um ON s.userID = um.userID;

CREATE VIEW GenreScore AS
WITH scores AS (
    SELECT 
        ut.userID as userID,
        ut.genreID as genreID,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM "User" u
    INNER JOIN Score s ON u.ID = s.userID
    INNER JOIN BookGenre bg ON s.bookID = bg.bookID
    RIGHT JOIN (SELECT u.ID as userID, g.ID as genreID FROM "User" u CROSS JOIN Genre g) ut ON u.ID = ut.userID AND bg.genreID = ut.genreID
    WHERE s.isactual = TRUE
    GROUP BY ut.userID, ut.genreID
),
user_maxima AS (
    SELECT 
        userID,
        MAX(score) as max_score,
        MAX(votes) as max_votes
    FROM scores
    GROUP BY userID
)
SELECT
    s.userID,
    s.genreID,
    CASE 
        WHEN um.max_score > 0 THEN s.score / um.max_score 
        ELSE 0 
    END as score,
    CASE 
        WHEN um.max_votes > 0 THEN s.votes::float / um.max_votes 
        ELSE 0 
    END as votes
FROM scores s
JOIN user_maxima um ON s.userID = um.userID;

CREATE VIEW TagScore AS
WITH scores AS (
    SELECT 
        ut.userID as userID,
        ut.tagID as tagID,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM "User" u
    INNER JOIN Score s ON u.ID = s.userID
    INNER JOIN BookTag bt ON s.bookID = bt.bookID
    RIGHT JOIN (SELECT u.ID as userID, t.ID as tagID FROM "User" u CROSS JOIN Tag t) ut ON u.ID = ut.userID AND bt.tagID = ut.tagID
    WHERE s.isactual = TRUE
    GROUP BY ut.userID, ut.tagID
),
user_maxima AS (
    SELECT 
        userID,
        MAX(score) as max_score,
        MAX(votes) as max_votes
    FROM scores
    GROUP BY userID
)
SELECT
    s.userID,
    s.tagID,
    CASE 
        WHEN um.max_score > 0 THEN s.score / um.max_score 
        ELSE 0 
    END as score,
    CASE 
        WHEN um.max_votes > 0 THEN s.votes::float / um.max_votes 
        ELSE 0 
    END as votes
FROM scores s
JOIN user_maxima um ON s.userID = um.userID;

-- Materialized Views (refresh these periodically)
CREATE MATERIALIZED VIEW PersonalPart AS
SELECT 
    u.ID as userID,
    b.ID as bookID,
    (COALESCE(AVG(ts.score),0)*COALESCE(AVG(ts.votes),0) + 
     COALESCE(AVG(gs.score),0)*COALESCE(AVG(gs.votes),0) + 
     COALESCE(AVG(tgs.score),0)*COALESCE(AVG(tgs.votes),0)) / 3 as compatibility
FROM "User" u
CROSS JOIN Book b
LEFT JOIN BookType bt ON b.ID = bt.bookID
LEFT JOIN BookGenre bg ON b.ID = bg.bookID
LEFT JOIN BookTag btg ON b.ID = btg.bookID
LEFT JOIN TypeScore ts ON u.ID = ts.userID AND bt.typeID = ts.typeID
LEFT JOIN GenreScore gs ON u.ID = gs.userID AND bg.genreID = gs.genreID
LEFT JOIN TagScore tgs ON u.ID = tgs.userID AND btg.tagID = tgs.tagID
GROUP BY u.ID, b.ID
DISTRIBUTED BY (userID, bookID);

CREATE MATERIALIZED VIEW Top AS
WITH scores AS (
    SELECT 
        b.ID as bookID,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM Book b
    LEFT JOIN Score s ON b.ID = s.bookID AND s.isactual = TRUE
    GROUP BY b.ID
),
normalized AS (
    SELECT
        s.bookID,
        COALESCE(s.score / NULLIF((SELECT MAX(s.score) as score FROM scores s), 0), 0) as normscore,
        COALESCE(s.votes::float / NULLIF((SELECT MAX(s.votes) as votes FROM scores s), 0), 0) as normvotes
    FROM scores s
)
SELECT 
    n.bookID as bookID,
    ROW_NUMBER() OVER (ORDER BY n.normscore * n.normvotes DESC) as rank,
    n.normscore * n.normvotes as compscore
FROM normalized n
DISTRIBUTED BY (bookID);

CREATE MATERIALIZED VIEW weeklyTop AS
WITH weekscores AS (
    SELECT 
        b.ID as bookID,
        AVG(s.score) as score,
        COUNT(s.score) as votes
    FROM Book b
    LEFT JOIN Score s ON b.ID = s.bookID
    WHERE s.isactual = TRUE AND s.updatets >= NOW() - INTERVAL '7' DAY
    GROUP BY b.ID
),
normalized AS (
    SELECT
        s.bookID,
        COALESCE(s.score / NULLIF((SELECT MAX(s.score) as score FROM weekscores s), 0), 0) as normscore,
        COALESCE(s.votes::float / NULLIF((SELECT MAX(s.votes) as votes FROM weekscores s), 0), 0) as normvotes
    FROM weekscores s
)
SELECT 
    n.bookID as bookID,
    ROW_NUMBER() OVER (ORDER BY n.normscore * n.normvotes DESC) as rank,
    n.normscore * n.normvotes as compscore
FROM normalized n
DISTRIBUTED BY (bookID);

CREATE MATERIALIZED VIEW Recommendations AS
SELECT 
    pp.userID,
    pp.bookID,
    ROW_NUMBER() OVER (PARTITION BY pp.userID ORDER BY COALESCE(pp.compatibility,0) + COALESCE(t.compscore,0) DESC) as rank,
    (COALESCE(pp.compatibility,0) + COALESCE(t.compscore,0)) as compatibility
FROM PersonalPart pp
INNER JOIN Top t ON t.bookID = pp.bookID
WHERE (pp.userID, pp.bookID) NOT IN (SELECT userID, bookID FROM Completed WHERE isactual = TRUE)
DISTRIBUTED BY (userID, bookID);
