"""服务层维护的增量范围：只触碰本批改动前后涉及的车厢日。"""

from __future__ import annotations

import sqlite3

import pytest

from app.database import connect, init_db
from app.schemas import BatchIngestRequest
from app.service import ingest_batch


def _req(mileage, mounts=None, overhauls=None) -> BatchIngestRequest:
    return BatchIngestRequest(
        mileage=mileage, mounts=mounts or [], overhauls=overhauls or []
    )


@pytest.fixture()
def conn(tmp_path):
    c = connect(str(tmp_path / "serving.db"))
    init_db(c)
    yield c
    c.close()


def test_ingest_only_queries_affected_car_days(conn):
    ingest_batch(
        conn,
        _req(
            mileage=[
                {"record_no": "D-1", "car_id": "C1", "day": "2026-01-01", "meters": 10},
                {"record_no": "D-2", "car_id": "C1", "day": "2026-01-02", "meters": 20},
                {"record_no": "D-3", "car_id": "C9", "day": "2026-06-01", "meters": 90},
            ],
        ),
    )

    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        ingest_batch(
            conn,
            _req(
                mileage=[
                    # 只改 D-1：不碰 C9，也不全表扫描服务层
                    {"record_no": "D-1", "car_id": "C2",
                     "day": "2026-01-03", "meters": 10},
                ],
            ),
        )
    finally:
        conn.set_trace_callback(None)

    raw_selects = [
        s for s in statements
        if s.lstrip().upper().startswith("SELECT") and "FROM car_daily_mileage" in s
    ]
    assert len(raw_selects) == 2  # 本批记录号 + 批外同车厢日占用检查
    for sql in raw_selects:
        assert " IN " in sql  # 都按具体键值定位，不全表扫描

    serving_selects = [
        s for s in statements
        if s.lstrip().upper().startswith("SELECT") and "FROM car_mileage_day_serving" in s
    ]
    assert serving_selects
    for sql in serving_selects:
        assert "day IN" in sql  # 只按键值查本批涉及的车厢日，不全表扫描
    # 未涉及的 C9 服务行原样保留
    assert conn.execute(
        "SELECT meters FROM car_mileage_day_serving "
        "WHERE car_id = 'C9' AND day = '2026-06-01'"
    ).fetchone()["meters"] == 90


def test_block_split_then_refill_merges(conn):
    ingest_batch(
        conn,
        _req(
            mileage=[
                {"record_no": "D-1", "car_id": "C1", "day": "2026-02-01", "meters": 1},
                {"record_no": "D-2", "car_id": "C1", "day": "2026-02-02", "meters": 2},
                {"record_no": "D-3", "car_id": "C1", "day": "2026-02-03", "meters": 3},
            ],
        ),
    )
    rows = conn.execute(
        "SELECT start_day, end_day FROM car_mileage_block WHERE car_id = 'C1'"
    ).fetchall()
    assert [tuple(r) for r in rows] == [("2026-02-01", "2026-02-04")]

    # 把中间一天整条挪走：连续块一分为二
    ingest_batch(
        conn,
        _req(
            mileage=[
                {"record_no": "D-2", "car_id": "C2",
                 "day": "2026-02-02", "meters": 2},
            ],
        ),
    )
    blocks = conn.execute(
        "SELECT start_day, end_day FROM car_mileage_block "
        "WHERE car_id = 'C1' ORDER BY start_day"
    ).fetchall()
    assert [tuple(r) for r in blocks] == [
        ("2026-02-01", "2026-02-02"),
        ("2026-02-03", "2026-02-04"),
    ]

    # 新记录补回该日：两块重新合并
    ingest_batch(
        conn,
        _req(
            mileage=[
                {"record_no": "D-4", "car_id": "C1",
                 "day": "2026-02-02", "meters": 0},
            ],
        ),
    )
    blocks = conn.execute(
        "SELECT start_day, end_day FROM car_mileage_block "
        "WHERE car_id = 'C1' ORDER BY start_day"
    ).fetchall()
    assert [tuple(r) for r in blocks] == [("2026-02-01", "2026-02-04")]
    assert conn.execute(
        "SELECT meters FROM car_mileage_day_serving "
        "WHERE car_id = 'C1' AND day = '2026-02-02'"
    ).fetchone()["meters"] == 0
