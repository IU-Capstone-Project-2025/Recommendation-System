-- Create tables
CREATE TABLE "User" (
    ID SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    codedpass VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Book (
    ID SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    author VARCHAR(100),
    year INT,
    score FLOAT,
    imgurl VARCHAR(1000),
    description TEXT,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Genre (
    ID SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Tag (
    ID SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Type (
    ID SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Score (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    score INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID)
);

CREATE TABLE Planned (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID)
);

CREATE TABLE Reading (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID)
);

CREATE TABLE Completed (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID),
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID)
);

CREATE TABLE Message (
    ID SERIAL PRIMARY KEY,
    userID INT NOT NULL,
    bookID INT NOT NULL,
    message TEXT NOT NULL,
    isactual BOOLEAN NOT NULL DEFAULT TRUE,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (userID) REFERENCES "User"(ID),
    FOREIGN KEY (bookID) REFERENCES Book(ID)
);

CREATE TABLE BookGenre (
    bookID INT NOT NULL,
    genreID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, genreID),
    FOREIGN KEY (bookID) REFERENCES Book(ID),
    FOREIGN KEY (genreID) REFERENCES Genre(ID)
);

CREATE TABLE BookTag (
    bookID INT NOT NULL,
    tagID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, tagID),
    FOREIGN KEY (bookID) REFERENCES Book(ID),
    FOREIGN KEY (tagID) REFERENCES Tag(ID)
);

CREATE TABLE BookType (
    bookID INT NOT NULL,
    typeID INT NOT NULL,
    updatets TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, typeID),
    FOREIGN KEY (bookID) REFERENCES Book(ID),
    FOREIGN KEY (typeID) REFERENCES Type(ID)
);

-- Create indexes
CREATE INDEX score_user_btree ON Score(userID);
CREATE INDEX score_book_btree ON Score(bookID);

CREATE INDEX planned_user_btree ON Planned(userID);
CREATE INDEX reading_user_btree ON Reading(userID);
CREATE INDEX completed_user_btree ON Completed(userID);

CREATE INDEX message_user_btree ON Message(userID);
CREATE INDEX bookgenre_book_btree ON BookGenre(bookID);
CREATE INDEX booktag_book_btree ON BookTag(bookID);
CREATE INDEX booktype_book_btree ON BookType(bookID);

-- Insert initial data (optional)
-- INSERT INTO Genre (name) VALUES ('Fiction'), ('Non-Fiction'), ('Science Fiction'), ('Fantasy'), ('Mystery');
