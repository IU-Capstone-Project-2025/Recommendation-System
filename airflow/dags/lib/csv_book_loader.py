from logging import Logger
import csv

from lib.pg_connect import PgConnect


class CSVBookLoader:
    """
    A class to load book data from CSV files into PostgreSQL database.
    Handles books, their types, genres, and tags with proper relationships.
    """

    def __init__(self, csv_file: str, pg_dest: PgConnect, log: Logger) -> None:
        """
        Initialize the CSV loader with source file and database connection.

        Args:
            csv_file: Path to the CSV file containing book data
            pg_dest: PostgreSQL connection wrapper
            log: Logger instance for tracking operations
        """
        self.csv_file = csv_file
        self.pg_dest = pg_dest
        self.log = log

    def load_book(self) -> None:
        """
        Main method to load book data from CSV to PostgreSQL.

        Steps:
        1. Opens CSV file and reads as dictionary
        2. For each book record:
           - Inserts/updates book type
           - Inserts/updates book details
           - Links book to its type
           - Processes genres and creates relationships
           - Processes tags (subject categories) and creates relationships
        3. Commits the transaction
        """
        with self.pg_dest.connection() as conn:
            # Open CSV file for reading with UTF-8 encoding
            with open(self.csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(
                    f
                )  # Read CSV as dictionary for easier column access

                for row in reader:
                    with conn.cursor() as cur:
                        # 1. Handle Book Type (upsert and get ID)
                        type_name = row["Type"].strip()
                        cur.execute(
                            """
                            INSERT INTO Type (name) 
                            VALUES (%s) 
                            ON CONFLICT (name) 
                            DO UPDATE SET name = EXCLUDED.name 
                            RETURNING ID
                            """,
                            (type_name,),
                        )
                        type_id = cur.fetchone()[0]

                        # 2. Insert/Update Book details
                        cur.execute(
                            """
                            INSERT INTO Book (title, author, year, imgurl, description)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (title) 
                            DO UPDATE SET 
                                author = EXCLUDED.author,
                                year = EXCLUDED.year,
                                imgurl = EXCLUDED.imgurl,
                                description = EXCLUDED.description
                            RETURNING ID
                            """,
                            (
                                row["Title"],
                                row["Authors"],
                                int(row["Published Date"]),
                                row["Image"],
                                row["Description"],
                            ),
                        )
                        book_id = cur.fetchone()[0]

                        # 3. Create Book-Type relationship
                        cur.execute(
                            """
                            INSERT INTO BookType (bookID, typeID) 
                            VALUES (%s, %s) 
                            ON CONFLICT (bookID, typeID) 
                            DO NOTHING
                            """,
                            (book_id, type_id),
                        )

                    # 4. Process Genre Categories (comma-separated)
                    genres = [g.strip() for g in row["Genre Categories"].split(",")]
                    with conn.cursor() as cur:
                        for genre_name in genres:
                            # Upsert genre and get ID
                            cur.execute(
                                """
                                INSERT INTO Genre (name) 
                                VALUES (%s) 
                                ON CONFLICT (name) 
                                DO UPDATE SET name = EXCLUDED.name 
                                RETURNING ID
                                """,
                                (genre_name,),
                            )
                            genre_id = cur.fetchone()[0]

                            # Create Book-Genre relationship
                            cur.execute(
                                """
                                INSERT INTO BookGenre (bookID, genreID) 
                                VALUES (%s, %s) 
                                ON CONFLICT (bookID, genreID) 
                                DO NOTHING
                                """,
                                (book_id, genre_id),
                            )

                    # 5. Process Subject Categories as Tags (comma-separated)
                    tags = [t.strip() for t in row["Subject Categories"].split(",")]
                    with conn.cursor() as cur:
                        for tag_name in tags:
                            # Upsert tag and get ID
                            cur.execute(
                                """
                                INSERT INTO Tag (name) 
                                VALUES (%s) 
                                ON CONFLICT (name) 
                                DO UPDATE SET name = EXCLUDED.name 
                                RETURNING ID
                                """,
                                (tag_name,),
                            )
                            tag_id = cur.fetchone()[0]

                            # Create Book-Tag relationship
                            cur.execute(
                                """
                                INSERT INTO BookTag (bookID, tagID) 
                                VALUES (%s, %s) 
                                ON CONFLICT (bookID, tagID) 
                                DO NOTHING
                                """,
                                (book_id, tag_id),
                            )

            # Commit all changes as a single transaction
            conn.commit()
            self.log.info("Data loaded successfully!")
