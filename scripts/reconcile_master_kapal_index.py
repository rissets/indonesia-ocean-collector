"""Reconcile master_kapal transmitter/BKP from KKP search index."""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("KKP_DATAMART_BASE_URL", "https://insight.kkp.go.id/datamart/api")
PLACEHOLDERS = {"", "-", "0", "0.0", "0.00", "none", "null", "n/a", "na", "unknown", "undefined", "tidak diketahui"}
UNKNOWN_TEXT = "Tidak diketahui"


def _clean_text(value: Any, max_len: int | None = None) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if text.lower() in PLACEHOLDERS:
        return None
    return text[:max_len] if max_len else text


def _get_db_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "217.217.252.155"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "maritime-os"),
        user=os.getenv("DB_USER", "maritime-os"),
        password=os.getenv("DB_PASSWORD", "@Maritime210526"),
        connect_timeout=15,
    )


def _headers() -> dict[str, str]:
    token = os.getenv("KKP_DATAMART_TOKEN")
    if not token:
        raise RuntimeError("KKP_DATAMART_TOKEN is required")
    return {"Authorization": f"Bearer {token}"}


def fetch_index() -> list[dict[str, Any]]:
    resp = requests.get(
        f"{BASE_URL}/kapal/search-kapal-bkp",
        params={"page": 1, "limit": 100000, "sort_by": "nama_kapal", "sort_order": "asc"},
        headers=_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") or []
    return [row for row in data if isinstance(row, dict)]


def reconcile() -> dict[str, int]:
    rows = fetch_index()
    stats = {
        "source_rows": len(rows),
        "skipped_no_tx": 0,
        "exists_tx": 0,
        "updated_tx_by_bkp": 0,
        "inserted_new": 0,
        "insert_conflict": 0,
    }

    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            for idx, row in enumerate(rows, start=1):
                tx = _clean_text(row.get("transmitter_no") or row.get("nomor_transmitter"), max_len=100)
                bkp = _clean_text(row.get("nomor_buku_kapal") or row.get("no_bkp"), max_len=50)
                nama = _clean_text(row.get("nama_kapal"), max_len=150)

                if not tx:
                    stats["skipped_no_tx"] += 1
                    continue

                cur.execute("SELECT id FROM master_kapal WHERE no_transmitter = %s", (tx,))
                if cur.fetchone():
                    stats["exists_tx"] += 1
                    continue

                if bkp:
                    cur.execute("SELECT id FROM master_kapal WHERE nomor_bkp = %s", (bkp,))
                    by_bkp = cur.fetchone()
                else:
                    by_bkp = None

                if by_bkp:
                    cur.execute(
                        """
                        UPDATE master_kapal
                        SET
                            no_transmitter = %s,
                            nama_kapal = COALESCE(%s, nama_kapal),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (tx, nama, by_bkp[0]),
                    )
                    stats["updated_tx_by_bkp"] += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO master_kapal (
                            nama_kapal, nomor_bkp, tanda_selar, ukuran_kapal, no_transmitter,
                            pemilik, alat_tangkap, kekuatan_mesin, merek_mesin, wilayah_tangkap,
                            pelabuhan_pangkalan, aktif, created_at, updated_at
                        )
                        VALUES (
                            %s, %s, %s, 0, %s,
                            %s, %s, 0, %s, %s,
                            %s, TRUE, NOW(), NOW()
                        )
                        ON CONFLICT DO NOTHING
                        """,
                        (nama, bkp, UNKNOWN_TEXT, tx, UNKNOWN_TEXT, UNKNOWN_TEXT, UNKNOWN_TEXT, UNKNOWN_TEXT, UNKNOWN_TEXT),
                    )
                    if cur.rowcount > 0:
                        stats["inserted_new"] += 1
                    else:
                        stats["insert_conflict"] += 1

                if idx % 500 == 0:
                    conn.commit()
                    logger.info("Progress %d/%d: %s", idx, len(rows), stats)
        conn.commit()
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(reconcile())
