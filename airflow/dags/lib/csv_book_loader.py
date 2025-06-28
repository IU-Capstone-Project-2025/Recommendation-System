from logging import Logger
import csv

from lib.pg_connect import PgConnect


class CSVBookLoader:
    def __init__(self, csv_file: str, pg_dest: PgConnect, log: Logger) -> None:
        self.csv_file = csv_file
        self.pg_dest = pg_dest
        self.log = log

    def load_book(self):
        with self.pg_dest.connection() as conn:
            
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    with conn.cursor() as cur:
                        # Insert or get Type
                        type_name = row['Type'].strip()
                        cur.execute(
                            "INSERT INTO Type (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING ID",
                            (type_name,)
                        )
                        type_id = cur.fetchone()[0]
                        
                        # Insert Book
                        cur.execute(
                            """INSERT INTO Book (title, author, year, imgurl, description, score, votes)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            RETURNING ID""",
                            (row['Title'], row['Authors'], int(row['Published Date']), 
                            row['Image'], row['Description'], 0.0, 0)
                        )
                        book_id = cur.fetchone()[0]
                        
                        # Link Book to Type
                        cur.execute(
                            "INSERT INTO BookType (bookID, typeID) VALUES (%s, %s)",
                            (book_id, type_id)
                        )
                        
                    # Process Genre Categories
                    genres = [g.strip() for g in row['Genre Categories'].split(',')]
                    with conn.cursor() as cur:
                        for genre_name in genres:
                            cur.execute(
                                "INSERT INTO Genre (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING ID",
                                (genre_name,)
                            )
                            genre_id = cur.fetchone()[0]
                            
                            cur.execute(
                                "INSERT INTO BookGenre (bookID, genreID) VALUES (%s, %s)",
                                (book_id, genre_id)
                            )
                        
                    # Process Subject Categories (as Tags)
                    tags = [t.strip() for t in row['Subject Categories'].split(',')]
                    with conn.cursor() as cur:
                        for tag_name in tags:
                            cur.execute(
                                "INSERT INTO Tag (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING ID",
                                (tag_name,)
                            )
                            tag_id = cur.fetchone()[0]
                            
                            cur.execute(
                                "INSERT INTO BookTag (bookID, tagID) VALUES (%s, %s)",
                                (book_id, tag_id)
                            )
            
            # Commit the transaction
            conn.commit()
            self.log.info("Data loaded successfully!")
