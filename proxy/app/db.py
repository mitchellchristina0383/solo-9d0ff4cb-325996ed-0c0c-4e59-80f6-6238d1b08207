"""代理侧 SQLite 持久层：节点配置、当前路由、写入记录、事件/变更记录。

关键不变量：
  * 写入记录一旦落库即代表"已受理/已送达某节点"，回执状态只能就地向原节点查询更新，
    任何状态机代码都不会把同一条写入再投到另一个节点。
  * 路由变更与 SWITCH_OK 变更事件、写入落库与 WRITE_ACCEPTED 事件、写入结论变更与
    对应结论事件，都在**同一个 SQLite 事务**里提交。事务失败则两者一起回滚，
    不会出现"状态变了但没事件"或"有事件但状态没变"。
  * events.id 是持久、单调递增的变更游标。写事务由单连接 + 单锁串行提交，
    因此按 id 升序分页就是按提交顺序分页，可用 `id > after` 续看且不漏不重。
  * 普通日志事件（BOOT / 切流被拦截 / 故障注入）也写入 events，但 change=0，
    不出现在 /api/changes 的变更记录里；它们不对应路由或写入状态变化。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable

import aiosqlite

DB_PATH = os.environ.get("PROXY_DB", "/data/proxy.db")

# 写入状态机
PENDING = "PENDING"        # 已受理/投递中，尚未拿到回执
ACKED = "ACKED"            # 已拿到原节点成功回执
TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"  # 写入已落库但回执超时，结果不确定
FAILED = "FAILED"          # 节点明确拒绝（连接前失败/5xx），未送达


class DuplicateWrite(Exception):
    """客户端提供了已存在的写入 ID；不重投，也不生成虚假的受理事件。"""

    def __init__(self, write_id: str) -> None:
        super().__init__(f"write id already exists: {write_id}")
        self.write_id = write_id


# 单写连接 + 单写锁：SQLite 写入串行化，保证 (状态更新, 变更事件) 原子提交，
# 同时保证 AUTOINCREMENT 事件 id 的分配顺序就是提交顺序。
_conn: aiosqlite.Connection | None = None
_write_lock: asyncio.Lock | None = None


@asynccontextmanager
async def _read_conn() -> AsyncIterator[aiosqlite.Connection]:
    """读取使用独立连接：WAL 快照只可见已提交事务，绝不会读到写事务的中间态。"""
    conn = await aiosqlite.connect(DB_PATH)
    try:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA busy_timeout=5000")
        yield conn
    finally:
        await conn.close()


async def _rollback_quietly() -> None:
    assert _conn is not None
    try:
        await _conn.rollback()
    except Exception:
        # 没有活动事务时 rollback 是无害的；这里只保护异常路径。
        pass


async def init_db() -> None:
    global _conn, _write_lock
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    _conn = await aiosqlite.connect(DB_PATH)
    _write_lock = asyncio.Lock()
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    # 持久游标要能跨进程重启继续；FULL 保证提交返回后事务已落盘。
    await _conn.execute("PRAGMA synchronous=FULL")
    await _conn.execute("PRAGMA busy_timeout=5000")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            name        TEXT PRIMARY KEY,
            base_url    TEXT NOT NULL,
            created_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            active      TEXT NOT NULL,
            updated_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            id           TEXT PRIMARY KEY,
            target_node  TEXT NOT NULL,
            payload      TEXT NOT NULL,
            delay_hint   REAL NOT NULL DEFAULT 0,
            status       TEXT NOT NULL,
            status_detail TEXT NOT NULL DEFAULT '',
            created_at   REAL NOT NULL,
            updated_at   REAL NOT NULL,
            acked_at     REAL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL NOT NULL,
            kind        TEXT NOT NULL,
            message     TEXT NOT NULL,
            node        TEXT,
            write_id    TEXT,
            state       TEXT,
            change      INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    # 兼容已有卷里的旧 events 表（只增加列，不改动历史行）。
    cur = await _conn.execute("PRAGMA table_info(events)")
    existing = {row["name"] for row in await cur.fetchall()}
    for column, decl in (
        ("node", "TEXT"),
        ("write_id", "TEXT"),
        ("state", "TEXT"),
        ("change", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if column not in existing:
            await _conn.execute(f"ALTER TABLE events ADD COLUMN {column} {decl}")
    await _conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_change_id ON events(change, id)"
    )
    await _conn.commit()


async def register_node(name: str, base_url: str) -> None:
    assert _conn is not None and _write_lock is not None
    async with _write_lock:
        try:
            await _conn.execute(
                "INSERT OR IGNORE INTO nodes (name, base_url, created_at) VALUES (?, ?, ?)",
                (name, base_url, time.time()),
            )
            # 已存在则更新地址（compose 重建容器场景）。
            await _conn.execute(
                "UPDATE nodes SET base_url = ? WHERE name = ?", (base_url, name)
            )
            cur = await _conn.execute("SELECT COUNT(*) AS c FROM route")
            row = await cur.fetchone()
            if row["c"] == 0:
                await _conn.execute(
                    "INSERT INTO route (id, active, updated_at) VALUES (1, ?, ?)",
                    (name, time.time()),
                )
            await _conn.commit()
        except BaseException:
            await _rollback_quietly()
            raise


async def get_active() -> str:
    async with _read_conn() as conn:
        cur = await conn.execute("SELECT active FROM route WHERE id = 1")
        row = await cur.fetchone()
        return row["active"]


async def commit_switch(
    target: str, message_for_previous: Callable[[str], str]
) -> tuple[bool, str]:
    """原子提交切流：比较并交换当前路由 + 写 SWITCH_OK 变更事件。

    返回 (是否真的改变了路由, 提交时的前一个现役节点)。
    并发切到同一目标时只有一个请求能改变路由，其余返回 noop，不会产生重复事件。
    message_for_previous 以事务内读到的真实前一个节点生成事件说明。
    """
    assert _conn is not None and _write_lock is not None
    async with _write_lock:
        try:
            cur = await _conn.execute("SELECT active FROM route WHERE id = 1")
            row = await cur.fetchone()
            previous = row["active"]
            if previous == target:
                # 没有生效的变化：不写事件。
                return False, previous

            now = time.time()
            await _conn.execute(
                "UPDATE route SET active = ?, updated_at = ? WHERE id = 1",
                (target, now),
            )
            await _conn.execute(
                "INSERT INTO events "
                "(ts, kind, message, node, write_id, state, change) "
                "VALUES (?, ?, ?, ?, NULL, ?, 1)",
                (
                    now,
                    "SWITCH_OK",
                    message_for_previous(previous),
                    target,
                    target,
                ),
            )
            await _conn.commit()
            return True, previous
        except BaseException:
            await _rollback_quietly()
            raise


async def list_nodes() -> list[dict[str, Any]]:
    async with _read_conn() as conn:
        cur = await conn.execute("SELECT name, base_url FROM nodes ORDER BY name")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def node_url(name: str) -> str | None:
    async with _read_conn() as conn:
        cur = await conn.execute(
            "SELECT base_url FROM nodes WHERE name = ?", (name,)
        )
        row = await cur.fetchone()
        return row["base_url"] if row else None


async def insert_write(
    write_id: str, target_node: str, payload: str, delay_hint: float
) -> None:
    """原子提交"写入受理"：writes 行 + WRITE_ACCEPTED 变更事件同一事务。"""
    assert _conn is not None and _write_lock is not None
    async with _write_lock:
        try:
            now = time.time()
            await _conn.execute(
                "INSERT INTO writes (id, target_node, payload, delay_hint, status, "
                "status_detail, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    write_id,
                    target_node,
                    payload,
                    delay_hint,
                    PENDING,
                    "accepted, awaiting ack",
                    now,
                    now,
                ),
            )
            await _conn.execute(
                "INSERT INTO events "
                "(ts, kind, message, node, write_id, state, change) "
                "VALUES (?, ?, ?, ?, ?, ?, 1)",
                (
                    now,
                    "WRITE_ACCEPTED",
                    f"{write_id} accepted for delivery to {target_node}",
                    target_node,
                    write_id,
                    PENDING,
                ),
            )
            await _conn.commit()
        except aiosqlite.IntegrityError as exc:
            await _rollback_quietly()
            # 仅当同 ID 的写入已存在时才视为客户端重复 ID；其他完整性错误
            #（例如注入的触发器）按真实失败上抛，绝不伪装成"已受理"。
            cur = await _conn.execute(
                "SELECT 1 FROM writes WHERE id = ?", (write_id,)
            )
            if await cur.fetchone() is not None:
                raise DuplicateWrite(write_id) from exc
            raise
        except BaseException:
            await _rollback_quietly()
            raise


async def transition_write(
    write_id: str,
    status: str,
    detail: str,
    *,
    event_kind: str,
    event_message: str,
    acked: bool = False,
) -> bool:
    """原子提交写入结论变化：仅在状态确实改变时更新并写一条变更事件。

    返回是否发生了结论变化。状态没变（例如核实后仍是原结论）时不会产生新事件。
    """
    assert _conn is not None and _write_lock is not None
    async with _write_lock:
        try:
            cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
            row = await cur.fetchone()
            if row is None:
                return False
            if row["status"] == status:
                # 结论未改变：不更新、不写事件。
                return False
            if row["status"] == ACKED:
                # ACKED 是终态：并发核实已确认成功后，在途请求的迟到超时/失败
                # 不得把结论回退，也不得生成"结论改变"事件。
                return False

            now = time.time()
            if acked:
                await _conn.execute(
                    "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
                    "acked_at = ? WHERE id = ?",
                    (status, detail, now, now, write_id),
                )
            else:
                await _conn.execute(
                    "UPDATE writes SET status = ?, status_detail = ?, updated_at = ? "
                    "WHERE id = ?",
                    (status, detail, now, write_id),
                )
            await _conn.execute(
                "INSERT INTO events "
                "(ts, kind, message, node, write_id, state, change) "
                "VALUES (?, ?, ?, ?, ?, ?, 1)",
                (
                    now,
                    event_kind,
                    event_message,
                    row["target_node"],
                    write_id,
                    status,
                ),
            )
            await _conn.commit()
            return True
        except BaseException:
            await _rollback_quietly()
            raise


async def update_write_detail(write_id: str, detail: str) -> None:
    """只更新说明文字，不改变结论，也不生成变更事件。"""
    assert _conn is not None and _write_lock is not None
    async with _write_lock:
        try:
            await _conn.execute(
                "UPDATE writes SET status_detail = ?, updated_at = ? WHERE id = ?",
                (detail, time.time(), write_id),
            )
            await _conn.commit()
        except BaseException:
            await _rollback_quietly()
            raise


async def get_write(write_id: str) -> dict[str, Any] | None:
    async with _read_conn() as conn:
        cur = await conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def list_writes() -> list[dict[str, Any]]:
    async with _read_conn() as conn:
        cur = await conn.execute(
            "SELECT * FROM writes ORDER BY created_at DESC LIMIT 200"
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def add_event(
    kind: str,
    message: str,
    *,
    node: str | None = None,
    write_id: str | None = None,
    state: str | None = None,
) -> None:
    """写普通日志事件（不对应路由/写入状态变化，change=0）。"""
    assert _conn is not None and _write_lock is not None
    async with _write_lock:
        try:
            await _conn.execute(
                "INSERT INTO events "
                "(ts, kind, message, node, write_id, state, change) "
                "VALUES (?, ?, ?, ?, ?, ?, 0)",
                (time.time(), kind, message, node, write_id, state),
            )
            await _conn.commit()
        except BaseException:
            await _rollback_quietly()
            raise


async def list_events(limit: int = 30) -> list[dict[str, Any]]:
    """最近事件（保留原有控制台行为）。"""
    async with _read_conn() as conn:
        cur = await conn.execute(
            "SELECT id, ts, kind, message, node, write_id, state "
            "FROM events ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def latest_change_id() -> int:
    async with _read_conn() as conn:
        cur = await conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS latest FROM events WHERE change = 1"
        )
        return int((await cur.fetchone())["latest"])


async def list_changes(
    after: int = 0, limit: int = 100
) -> tuple[list[dict[str, Any]], bool, int]:
    """按持久游标 after（不含）升序翻页读取真正的变更记录。

    返回 (events, has_more, latest_cursor)。events 中每条都有 id / ts / kind /
    node / write_id / state / message。commit 由单写连接串行，id 顺序即提交顺序。
    """
    async with _read_conn() as conn:
        # 分页与 latest 用同一个读事务/快照，二者不会互相矛盾：
        # has_more=false 时 latest 恰好就是本页最后一条（或 0）。
        await conn.execute("BEGIN")
        try:
            cur = await conn.execute(
                "SELECT id, ts, kind, message, node, write_id, state "
                "FROM events WHERE change = 1 AND id > ? ORDER BY id ASC LIMIT ?",
                (after, limit + 1),
            )
            rows = [dict(r) for r in await cur.fetchall()]
            has_more = len(rows) > limit
            rows = rows[:limit]

            cur = await conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS latest FROM events WHERE change = 1"
            )
            latest = int((await cur.fetchone())["latest"])
        finally:
            await conn.rollback()
        return rows, has_more, latest
