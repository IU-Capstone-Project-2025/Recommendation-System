from src.scripts.pg_connect import PgConnectionBuilder


class BookStats:
    bookId: int
    scores: list[int | float]
    statuses: list[int]

    def __init__(self, bookId: int):
        self.bookId = bookId
        self._db = PgConnectionBuilder.pg_conn()
        self.scores = self.get_scores()
        self.statuses = self.get_statuses()

    def get_scores(self) -> list[int | float]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN score = 1 THEN 1 ELSE 0 END) AS one_score,
                    SUM(CASE WHEN score = 2 THEN 1 ELSE 0 END) AS two_score,
                    SUM(CASE WHEN score = 3 THEN 1 ELSE 0 END) AS three_score,
                    SUM(CASE WHEN score = 4 THEN 1 ELSE 0 END) AS four_score,
                    SUM(CASE WHEN score = 5 THEN 1 ELSE 0 END) AS five_score,
                    AVG(score) AS avg_score
                FROM score
                WHERE bookid = %(bookid)s AND isactual = true
                """,
                {"bookid": self.bookId},
            )

            res: tuple[int, int, int, int, int, int, float] = (
                cur.fetchone()
            )  # pyright: ignore type

            scores = [
                int(res[i] / res[0] * 100) if res[0] else 0 for i in range(1, 6)
            ] + [round(res[6] if res[6] else 0, 1)]

            return scores

    def get_statuses(self) -> list[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                WITH statuses AS (
                    SELECT
                        'completed' AS status
                    FROM completed
                    WHERE bookid = %(bookid)s AND isactual = true
                    UNION ALL
                    SELECT
                        'reading' AS status
                    FROM reading
                    WHERE bookid = %(bookid)s AND isactual = true
                    UNION ALL
                    SELECT
                        'planned' AS status
                    FROM planned
                    WHERE bookid = %(bookid)s AND isactual = true
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 'reading' THEN 1 ELSE 0 END) AS reading,
                    SUM(CASE WHEN status = 'planned' THEN 1 ELSE 0 END) AS planned
                FROM statuses
                """,
                {"bookid": self.bookId},
            )

            res: tuple[int, int, int, int] = cur.fetchone()  # pyright: ignore type

            scores = [int(res[i] / res[0] * 100) if res[0] else 0 for i in range(1, 4)]

            return scores
