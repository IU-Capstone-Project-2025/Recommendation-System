/*
 BOOK RECOMMENDATION SYSTEM DATABASE SCHEMA
 
 This schema defines the database structure for a book recommendation system with:
 - User management
 - Book catalog with metadata
 - Categorization through genres, tags, and types
 - User engagement tracking (ratings, reading status)
 - Messaging system
 - Relationship mapping between entities
 
 All tables include automatic timestamp tracking (updatets) for change monitoring.
 */
-- Core User Table
CREATE TABLE "User" (
    ID SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    -- Unique username for login/display
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last update timestamp
);

-- Book Catalog Table
CREATE TABLE Book (
    ID SERIAL PRIMARY KEY,
    title VARCHAR(100) UNIQUE NOT NULL,
    -- Book title with uniqueness constraint
    author VARCHAR(100),
    -- Author name
    year INT,
    -- Publication year
    imgurl VARCHAR(1000),
    -- URL for book cover image
    description TEXT,
    -- Detailed book description
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last update timestamp
);

-- Categorization Tables
CREATE TABLE Genre (
    ID SERIAL PRIMARY KEY,
    NAME VARCHAR(100) UNIQUE NOT NULL,
    -- Genre name (e.g., "Science Fiction")
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Tag (
    ID SERIAL PRIMARY KEY,
    NAME VARCHAR(100) UNIQUE NOT NULL,
    -- Descriptive tags (e.g., "AI", "Space Opera")
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE TYPE (
    ID SERIAL PRIMARY KEY,
    NAME VARCHAR(100) UNIQUE NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- User Engagement Tables
CREATE TABLE Score (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    score INT NOT NULL,
    -- Rating score (1-5)
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    -- Flag for current/active ratings
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    -- Composite primary key
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE Planned (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    -- Tracks current reading plans
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE Reading (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    -- Current reading status
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE Completed (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    -- Tracks completed reads
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

-- User Communication Table
CREATE TABLE Message (
    ID SERIAL PRIMARY KEY,
    userID INT NOT NULL,
    -- Message author
    bookID INT NOT NULL,
    -- Related book
    message TEXT NOT NULL,
    -- Message content
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    -- Flag for deleted messages
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

-- Relationship Mapping Tables
CREATE TABLE BookGenre (
    bookID INT NOT NULL,
    genreID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, genreID),
    -- Many-to-many relationship
    FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (genreID) REFERENCES Genre(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE BookTag (
    bookID INT NOT NULL,
    tagID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, tagID),
    FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (tagID) REFERENCES Tag(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE BookType (
    bookID INT NOT NULL,
    typeID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, typeID),
    FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (typeID) REFERENCES TYPE(ID) ON
    DELETE
        CASCADE
);

/*
 PERFORMANCE OPTIMIZATION INDEXES
 
 The following indexes are created to optimize queries for:
 - User activity lookups
 - Book relationship navigation
 - Recommendation calculations
 */
-- User activity indexes
CREATE INDEX score_user_btree ON Score(userID);

CREATE INDEX score_book_btree ON Score(bookID);

CREATE INDEX planned_user_btree ON Planned(userID);

CREATE INDEX reading_user_btree ON Reading(userID);

CREATE INDEX completed_user_btree ON Completed(userID);

-- Book relationship indexes
CREATE INDEX message_user_btree ON Message(userID);

CREATE INDEX bookgenre_book_btree ON BookGenre(bookID);

CREATE INDEX booktag_book_btree ON BookTag(bookID);

CREATE INDEX booktype_book_btree ON BookType(bookID);

/*
 INITIAL REFERENCE DATA
 
 Optional seed data for system categories.
 Can be extended with more genres/tags/types as needed.
 */
-- INSERT INTO Genre (name) VALUES 
--   ('Fiction'), 
--   ('Non-Fiction'), 
--   ('Science Fiction'), 
--   ('Fantasy'), 
--   ('Mystery');
