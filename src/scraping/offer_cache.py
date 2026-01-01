import os
import sqlite3
import time
from typing import Optional, List, Dict, Any


class OfferCache:
    """Cache persistant (SQLite) des offres déjà vues, par profil utilisateur.

    Statuts recommandés :
    - PENDING_URL  : URL connue mais détails pas récupérés
    - DETAILED     : title/description stockés mais pas scorés
    - SCORED_WHITE : scoré + whitelist
    - SCORED_BLACK : scoré + blacklist
    - WHITE/BLACK/KNOWN : bootstrap historique (figé)
    - ERROR_DETAIL : erreur lors du fetch détail
    """

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA temp_store=MEMORY;")
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS offers (
                    offer_id    TEXT PRIMARY KEY,
                    source      TEXT NOT NULL,
                    url         TEXT NOT NULL,
                    title       TEXT,
                    description TEXT,
                    status      TEXT NOT NULL,
                    score       INTEGER,
                    is_good     INTEGER,
                    updated_at  INTEGER NOT NULL
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_offers_source ON offers(source);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_offers_status ON offers(status);")

    # ---------- Core API ----------

    def exists(self, offer_id: str) -> bool:
        if not offer_id:
            return False
        with self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM offers WHERE offer_id = ? LIMIT 1", (offer_id,)
            ).fetchone()
        return row is not None

    def get_status(self, offer_id: str) -> Optional[str]:
        if not offer_id:
            return None
        with self._connect() as con:
            row = con.execute(
                "SELECT status FROM offers WHERE offer_id = ?", (offer_id,)
            ).fetchone()
        return row["status"] if row else None

    def get_offer(self, offer_id: str) -> Optional[Dict[str, Any]]:
        if not offer_id:
            return None
        with self._connect() as con:
            row = con.execute(
                "SELECT offer_id, source, url, title, description, status, score, is_good, updated_at "
                "FROM offers WHERE offer_id = ?",
                (offer_id,),
            ).fetchone()
        return dict(row) if row else None

    def upsert_url(self, offer_id: str, source: str, url: str, status: str) -> None:
        now = int(time.time())
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO offers(offer_id, source, url, status, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(offer_id) DO UPDATE SET
                    source=excluded.source,
                    url=excluded.url,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (offer_id, source, url, status, now),
            )

    def upsert_detail(
        self,
        offer_id: str,
        source: str,
        url: str,
        title: str,
        description: str,
        status: str = "DETAILED",
    ) -> None:
        now = int(time.time())
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO offers(offer_id, source, url, title, description, status, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(offer_id) DO UPDATE SET
                    source=excluded.source,
                    url=excluded.url,
                    title=excluded.title,
                    description=excluded.description,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (offer_id, source, url, title, description, status, now),
            )

    def set_scoring(self, offer_id: str, score: int, is_good: int, status: str) -> None:
        now = int(time.time())
        with self._connect() as con:
            con.execute(
                """
                UPDATE offers
                SET score=?, is_good=?, status=?, updated_at=?
                WHERE offer_id=?
                """,
                (int(score), int(is_good), status, now, offer_id),
            )

    def mark_error(self, offer_id: str, status: str = "ERROR_DETAIL") -> None:
        now = int(time.time())
        with self._connect() as con:
            con.execute(
                "UPDATE offers SET status=?, updated_at=? WHERE offer_id=?",
                (status, now, offer_id),
            )

    def should_fetch_detail(self, offer_id: str) -> bool:
        return not self.exists(offer_id)

    # ---------- Listing / reprise ----------

    def list_by_status(self, status: str, limit: int = 500) -> List[Dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT offer_id, source, url, title, description, status, score, is_good, updated_at
                FROM offers
                WHERE status = ?
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (status, int(limit)),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_not_scored(self, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT offer_id, source, url, title, description, status, score, is_good, updated_at
                FROM offers
                WHERE status IN ('DETAILED')
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------- Bootstrap ----------

    def bootstrap_ids(self, offer_ids, source: str, status: str) -> None:
        if not offer_ids:
            return
        now = int(time.time())
        rows = [(str(oid), source, "", status, now) for oid in offer_ids if oid]
        with self._connect() as con:
            con.executemany(
                """
                INSERT INTO offers(offer_id, source, url, status, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(offer_id) DO UPDATE SET
                    source=excluded.source,
                    url=excluded.url,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                rows,
            )

    # ---------- Rollback scoring KO ----------

    def rollback_scoring_black_to_detailed(self, limit: int = 100000) -> int:
        """
        Corrige le cas "LLM KO => score=0/is_good=0 => SCORED_BLACK".
        On repasse en DETAILED pour re-scoring plus tard.
        """
        now = int(time.time())
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT offer_id
                FROM offers
                WHERE status = 'SCORED_BLACK'
                  AND COALESCE(score, 0) = 0
                  AND COALESCE(is_good, 0) = 0
                  AND COALESCE(title, '') <> ''
                  AND COALESCE(description, '') <> ''
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

            ids = [r["offer_id"] for r in rows]
            if not ids:
                return 0

            con.executemany(
                "UPDATE offers SET status='DETAILED', score=NULL, is_good=NULL, updated_at=? WHERE offer_id=?",
                [(now, oid) for oid in ids],
            )
        return len(ids)

    # ---------- Stats ----------

    def count_by_status(self, status: str) -> int:
        with self._connect() as con:
            row = con.execute(
                "SELECT COUNT(1) AS n FROM offers WHERE status = ?", (status,)
            ).fetchone()
        return int(row["n"] or 0) if row else 0

    def count_by_statuses(self, statuses: List[str]) -> int:
        statuses = [s for s in (statuses or []) if s]
        if not statuses:
            return 0
        placeholders = ",".join(["?"] * len(statuses))
        with self._connect() as con:
            row = con.execute(
                f"SELECT COUNT(1) AS n FROM offers WHERE status IN ({placeholders})",
                tuple(statuses),
            ).fetchone()
        return int(row["n"] or 0) if row else 0
