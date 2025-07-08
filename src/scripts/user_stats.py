from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound


class UserStats:
    username: str
    userid: int
    scores: tuple[int, int, int, int, int]
    statuses: tuple[int, int, int, int]

    def __init__(self, username: str):
        self.username = username
        self._db = PgConnectionBuilder.pg_conn()
        self.userid = self.get_userid()
        self.scores = self.get_scores()
        self.statuses = self.get_statuses()

    def get_userid(self) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def get_scores(self) -> tuple[int, int, int, int, int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN score = 1 THEN 1 ELSE 0 END) AS one_score,
                    SUM(CASE WHEN score = 2 THEN 1 ELSE 0 END) AS two_score,
                    SUM(CASE WHEN score = 3 THEN 1 ELSE 0 END) AS three_score,
                    SUM(CASE WHEN score = 4 THEN 1 ELSE 0 END) AS four_score,
                    SUM(CASE WHEN score = 5 THEN 1 ELSE 0 END) AS five_score
                FROM score
                WHERE userid = %(userid)s AND isactual = true
                """,
                {"userid": self.userid},
            )

            res = cur.fetchone()
            scores = [int(res[i]/res[0] * 100) if res[0] else 0 for i in range(1, 6)]
            return scores

    def get_statuses(self) -> tuple[int, int, int, int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                WITH statuses AS (
                    SELECT
                        'completed' AS status
                    FROM completed
                    WHERE userid = %(userid)s AND isactual = true
                    UNION ALL
                    SELECT
                        'reading' AS status
                    FROM reading
                    WHERE userid = %(userid)s AND isactual = true
                    UNION ALL
                    SELECT
                        'planned' AS status
                    FROM planned
                    WHERE userid = %(userid)s AND isactual = true
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 'reading' THEN 1 ELSE 0 END) AS reading,
                    SUM(CASE WHEN status = 'planned' THEN 1 ELSE 0 END) AS planned
                FROM statuses
                """,
                {"userid": self.userid},
            )

            res = cur.fetchone()

            return res
