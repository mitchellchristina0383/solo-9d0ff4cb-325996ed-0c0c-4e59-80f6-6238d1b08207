#!/usr/bin/env python3
"""变更记录/持久游标专项端到端自测（仅标准库）。

覆盖：
  1. 写入受理、写入结论改变各产生一条变更事件（节点 / 写入 ID / 新状态）；
  2. 反复核实同一结论不产生新事件；
  3. 重复写入 ID 被拒绝，不产生虚假受理事件；
  4. no-op 切流、被探测拦截的切流不产生变更事件；
  5. 并发写入 + 并发切流交错：按游标分页（limit 很小）不漏不重、严格升序，
     事件数与真实生效的操作一致，最后一条 SWITCH_OK 与最终现役节点一致；
  6. 慢回执超时 -> TIMEOUT_UNCERTAIN；核实前后状态不变时不产生事件，改变时恰好一条；
  7. 代理重启后从旧游标继续，不重不漏；
  8. 用 SQLite 触发器注入事件写入失败：写入/切流/结论变更与事件同一事务回滚，
     不留下虚假事件，也不会出现"状态变了但没有事件"。
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            try:
                return resp.status, json.loads(raw)
            except Exception:
                return resp.status, {"raw": raw}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}
    except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}


def wait_ready(url: str, timeout: float = 25) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.2)
    return False


def fetch_page(proxy: str, after: int, limit: int) -> dict:
    code, data = http("GET", f"{proxy}/api/changes?after={after}&limit={limit}")
    assert code == 200, (code, data)
    return data


def all_changes(proxy: str, start: int = 0, limit: int = 500) -> list[dict]:
    out: list[dict] = []
    after = start
    for _ in range(1000):
        page = fetch_page(proxy, after, limit)
        out.extend(page["events"])
        if not page["has_more"]:
            break
        after = page["next_after"]
    return out


def status(proxy: str) -> dict:
    return http("GET", f"{proxy}/api/status")[1]


def write_status(proxy: str, write_id: str) -> str | None:
    for w in status(proxy)["writes"]:
        if w["id"] == write_id:
            return w["status"]
    return None


class Poster(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.code: int | None = None
        self.body: dict = {}

    def run(self):
        self.code, self.body = http(
            "POST", f"{self.proxy}/api/writes?delay={self.delay}",
            {"payload": self.payload}, timeout=30,
        )


class Switcher(threading.Thread):
    def __init__(self, proxy: str, target: str):
        super().__init__(daemon=True)
        self.proxy, self.target = proxy, target
        self.code: int | None = None
        self.body: dict = {}

    def run(self):
        self.code, self.body = http(
            "POST", f"{self.proxy}/api/switch", {"target": self.target}, timeout=30
        )


def clear_change_triggers(db_path: Path) -> None:
    try:
        con = sqlite3.connect(db_path, timeout=10)
        con.execute("DROP TRIGGER IF EXISTS test_fail_event")
        con.commit()
        con.close()
    except sqlite3.Error:
        pass


def install_change_trigger(db_path: Path, kind: str) -> None:
    clear_change_triggers(db_path)
    con = sqlite3.connect(db_path, timeout=10)
    con.execute(
        "CREATE TRIGGER test_fail_event BEFORE INSERT ON events "
        f"WHEN NEW.kind = '{kind}' "
        "BEGIN SELECT RAISE(ABORT, 'injected event failure'); END"
    )
    con.commit()
    con.close()


class FakeBackend:
    """可控的慢回执后端：用于制造"核实先确认 ACKED、迟到的读超时后到"的交错。"""

    def __init__(self, node: str):
        self.node = node
        self.rows: dict[str, dict] = {}
        self.lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code: int, body: dict):
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                if self.path == "/healthz":
                    return self._send(200, {"node": fake.node, "healthy": True})
                prefix = "/writes/"
                if self.path.startswith(prefix):
                    wid = self.path[len(prefix):]
                    with fake.lock:
                        row = fake.rows.get(wid)
                    if row is None:
                        return self._send(404, {"found": False})
                    return self._send(200, {**row, "node": fake.node,
                                            "found": True, "status": "acked"})
                return self._send(404, {})

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                wid = body.get("id")
                with fake.lock:
                    fake.rows[wid] = {"id": wid, "payload": body.get("payload")}
                # 落库后不立即回执：让代理的读超时先触发。
                time.sleep(4)
                return self._send(201, {"id": wid, "node": fake.node,
                                        "status": "acked"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-changes-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: list[subprocess.Popen] = []
    ports = {"proxy": 18100, "A": 18101, "B": 18102}
    proxy = f"http://127.0.0.1:{ports['proxy']}"
    proxy_db = work / "proxy.db"

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> subprocess.Popen:
        log = open(logs / f"{kind}.log", "ab")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(p)
        return p

    proxy_env = {
        "NODE_A_NAME": "A", "NODE_B_NAME": "B",
        "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
        "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
        "PROXY_DB": str(proxy_db),
        "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
    }
    proxy_proc: subprocess.Popen | None = None

    try:
        print("== 启动后端 A/B 与代理 ==")
        spawn("backend-a", ROOT / "backend",
              {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")}, ports["A"])
        spawn("backend-b", ROOT / "backend",
              {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")}, ports["B"])
        check(wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
              and wait_ready(f"http://127.0.0.1:{ports['B']}/healthz"),
              "两个后端启动并健康")
        proxy_proc = spawn("proxy", ROOT / "proxy", proxy_env, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理启动")

        print("\n== 场景 1：受理 / 回执两条变更，字段完整且顺序正确 ==")
        base = fetch_page(proxy, 0, 500)
        check(base["latest"] == 0 and base["events"] == [],
              "初始无变更事件（启动日志不算变更）", f"latest={base['latest']}")
        code, w1 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "first"})
        check(code == 201 and w1["status"] == "ACKED", "普通写入成功")
        evs = all_changes(proxy, 0)
        check([e["kind"] for e in evs] == ["WRITE_ACCEPTED", "WRITE_ACKED"],
              "产生且仅产生 受理+回执 两条变更", str([e["kind"] for e in evs]))
        check([e["id"] for e in evs] == sorted(e["id"] for e in evs),
              "变更 ID 严格升序")
        acc, ack = evs
        check(acc["write_id"] == w1["id"] and acc["node"] == "A"
              and acc["state"] == "PENDING",
              "受理事件记录节点、写入 ID、状态 PENDING")
        check(ack["write_id"] == w1["id"] and ack["node"] == "A"
              and ack["state"] == "ACKED",
              "回执事件记录节点、写入 ID、状态 ACKED")

        print("\n== 场景 2：核实未改变结论 -> 不生成新事件 ==")
        code, _ = http("GET", f"{proxy}/api/writes/{w1['id']}/refresh")
        code, _ = http("GET", f"{proxy}/api/writes/{w1['id']}/refresh")
        after_refresh = fetch_page(proxy, 0, 500)
        check(code == 200 and after_refresh["latest"] == evs[-1]["id"]
              and len(after_refresh["events"]) == 2,
              "已 ACKED 的写入反复核实不产生新变更")

        print("\n== 场景 3：重复写入 ID -> 409 且无虚假事件 ==")
        before = fetch_page(proxy, 0, 500)["latest"]
        code, dup = http("POST", f"{proxy}/api/writes?delay=0",
                         {"id": w1["id"], "payload": "duplicate"})
        after = fetch_page(proxy, 0, 500)
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        check(code == 409 and after["latest"] == before
              and sum(1 for r in rec_a["records"] if r["id"] == w1["id"]) == 1,
              "重复 ID 被拒绝，事件与后端记录都没有新增", f"code={code}")

        print("\n== 场景 4：no-op / 被拦截的切流不产生变更事件 ==")
        before = fetch_page(proxy, 0, 500)["latest"]
        code, noop = http("POST", f"{proxy}/api/switch", {"target": "A"})
        http("POST", f"{proxy}/api/nodes/B/fault", {"on": True})
        code_block, blocked = http("POST", f"{proxy}/api/switch", {"target": "B"})
        http("POST", f"{proxy}/api/nodes/B/fault", {"on": False})
        after = fetch_page(proxy, 0, 500)
        check(code == 200 and noop.get("noop") and after["latest"] == before,
              "切到已现役节点为 no-op，无变更事件")
        check(code_block == 409 and blocked.get("blocked")
              and after["latest"] == before,
              "探测失败切流 409，无变更事件且路由未变")

        print("\n== 场景 5：慢回执超时 -> 核实无变化不记，结论改变只记一次 ==")
        before = fetch_page(proxy, 0, 500)
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=4",
                          {"payload": "timeout"}, timeout=15)
        check(code == 504 and tout.get("status") == "TIMEOUT_UNCERTAIN",
              f"读超时返回 TIMEOUT_UNCERTAIN（{time.time()-t0:.1f}s）")
        # 立刻核实：后端还在慢回执中 -> pending，结论不变
        code, ref1 = http("GET", f"{proxy}/api/writes/{tout['id']}/refresh")
        mid = fetch_page(proxy, before["latest"], 500)
        kinds_mid = [e["kind"] for e in mid["events"]]
        check(code == 200 and ref1["write"]["status"] == "TIMEOUT_UNCERTAIN"
              and kinds_mid == ["WRITE_ACCEPTED", "WRITE_TIMEOUT_UNCERTAIN"],
              "后端尚未回执时核实不改结论、不写事件", str(kinds_mid))
        time.sleep(3.2)
        code, ref2 = http("GET", f"{proxy}/api/writes/{tout['id']}/refresh")
        after_resolve = fetch_page(proxy, before["latest"], 500)
        kinds = [e["kind"] for e in after_resolve["events"]]
        check(code == 200 and ref2["write"]["status"] == "ACKED"
              and kinds == ["WRITE_ACCEPTED", "WRITE_TIMEOUT_UNCERTAIN",
                            "WRITE_RESOLVED"],
              "核实确实改变结论时恰好新增一条 WRITE_RESOLVED", str(kinds))
        resolved = after_resolve["events"][-1]
        check(resolved["write_id"] == tout["id"] and resolved["state"] == "ACKED",
              "WRITE_RESOLVED 记录了写入 ID 与新状态 ACKED")
        http("GET", f"{proxy}/api/writes/{tout['id']}/refresh")
        check(fetch_page(proxy, before["latest"], 500)["latest"]
              == after_resolve["latest"],
              "再次核实同一结论不重复记录")

        print("\n== 场景 6：并发写入与切流交错，游标分页不漏不重 ==")
        # 记录场景 6 起点之前的所有变更，之后只核对新增部分。
        start_cursor = fetch_page(proxy, 0, 500)["latest"]
        end_cursor = None
        posters = [Poster(proxy, 0.25, f"conc-{i}") for i in range(14)]
        switchers = []
        for i in range(10):
            switchers.append(Switcher(proxy, "B" if i % 2 == 0 else "A"))
        threads: list[threading.Thread] = []
        # 让写入与切流在不同时刻发起，制造真实交错。
        for i, p in enumerate(posters):
            p.start()
            threads.append(p)
            if i % 2 == 0:
                s = switchers[i // 2]
                time.sleep(0.02)
                s.start()
                threads.append(s)
            time.sleep(0.03)
        for s in switchers[len(posters) // 2:]:
            s.start()
            threads.append(s)
        for t in threads:
            t.join(timeout=30)
        # 等全部慢回执结束、代理完成 ACKED 变更。
        deadline = time.time() + 15
        while time.time() < deadline:
            rows = status(proxy)["writes"]
            conc_ids = {p.body.get("id") for p in posters if p.body.get("id")}
            pending = [w for w in rows if w["id"] in conc_ids
                       and w["status"] == "PENDING"]
            if not pending:
                break
            time.sleep(0.2)
        end_cursor = fetch_page(proxy, 0, 500)["latest"]

        # 小页多次翻页读取整个新增区间。
        paged: list[dict] = []
        cursor = start_cursor
        pages = 0
        while True:
            page = fetch_page(proxy, cursor, 3)
            pages += 1
            paged.extend(page["events"])
            if not page["has_more"]:
                break
            check(page["next_after"] == page["events"][-1]["id"],
                  "next_after 是本页最后一条变更的 ID")
            cursor = page["next_after"]
            if pages > 200:
                break
        full = fetch_page(proxy, start_cursor, 500)["events"]
        paged_ids = [e["id"] for e in paged]
        full_ids = [e["id"] for e in full]
        check(paged_ids == full_ids,
              f"分页 {pages} 页与一次全量读取完全一致（不漏不重）",
              f"paged={len(paged_ids)} full={len(full_ids)}")
        check(paged_ids == sorted(set(paged_ids)),
              "分页结果严格升序且无重复")
        check(bool(paged) and paged[-1]["id"] == end_cursor,
              "翻页终点即最新变更游标", f"{end_cursor}")

        ok_switch = sum(1 for s in switchers
                        if s.code == 200 and s.body.get("ok")
                        and not s.body.get("noop"))
        switch_events = [e for e in full if e["kind"] == "SWITCH_OK"]
        check(len(switch_events) == ok_switch,
              "SWITCH_OK 事件数 = 实际生效切流数",
              f"events={len(switch_events)} effective={ok_switch}")
        final_active = status(proxy)["active"]
        check(not switch_events or switch_events[-1]["node"] == final_active,
              "最后一条 SWITCH_OK 与最终现役节点一致",
              f"last={switch_events[-1]['node'] if switch_events else '-'} "
              f"active={final_active}")
        check(all(e["node"] == "A" or e["node"] == "B" for e in switch_events),
              "切流事件记录了目标节点")

        conc_ids = {p.body.get("id") for p in posters if p.body.get("id")}
        accepted = [e for e in full if e["kind"] == "WRITE_ACCEPTED"
                    and e["write_id"] in conc_ids]
        check(len(accepted) == len(conc_ids) == 14,
              "每条并发写入恰好一条受理事件", f"{len(accepted)}/{len(conc_ids)}")
        # 每条写入的事件必须从 PENDING 开始，最终状态与 /api/status 一致。
        by_write: dict[str, list[dict]] = {}
        for e in full:
            if e["write_id"]:
                by_write.setdefault(e["write_id"], []).append(e)
        states_ok, terminal_ok = True, True
        status_by_id = {w["id"]: w["status"] for w in status(proxy)["writes"]}
        for wid in conc_ids:
            seq = [e["state"] for e in by_write.get(wid, [])]
            if not seq or seq[0] != "PENDING":
                states_ok = False
            if wid in status_by_id and seq and seq[-1] != status_by_id[wid]:
                terminal_ok = False
        check(states_ok, "每条并发写入的变更序列从 PENDING 开始")
        check(terminal_ok, "每条写入最后一条变更的新状态 = 当前写入状态")
        check(any(p.code == 201 for p in posters),
              "并发写入至少一部分成功回执")

        print("\n== 场景 6b：翻页的同时持续写入/核实，读到的流绝不重不漏不逆序 ==")
        page_start = fetch_page(proxy, 0, 500)["latest"]
        stream_ids: list[int] = []
        stop = threading.Event()
        errors: list[str] = []

        def reader():
            cursor = page_start
            try:
                while True:
                    page = fetch_page(proxy, cursor, 2)
                    for e in page["events"]:
                        stream_ids.append(e["id"])
                    if page["events"]:
                        cursor = page["next_after"]
                    if stop.is_set() and not page["has_more"]:
                        break
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{type(exc).__name__}: {exc}")

        rt = threading.Thread(target=reader, daemon=True)
        rt.start()
        live_posters = [Poster(proxy, 0.4, f"live-{i}") for i in range(6)]
        for p in live_posters:
            p.start()
            time.sleep(0.05)
        # 期间做一些核实（其中超时写入的核实会改变结论）。
        code, live_timeout = http("POST", f"{proxy}/api/writes?delay=3",
                                  {"payload": "live-timeout"}, timeout=15)
        http("GET", f"{proxy}/api/writes/{live_timeout['id']}/refresh")
        time.sleep(3.2)
        http("GET", f"{proxy}/api/writes/{live_timeout['id']}/refresh")
        for p in live_posters:
            p.join(timeout=30)
        time.sleep(0.6)
        stop.set()
        rt.join(timeout=15)
        check(not errors, "并发翻页期间没有读取错误", str(errors[:2]))
        check(stream_ids == sorted(stream_ids) and len(stream_ids) == len(set(stream_ids)),
              "并发翻页读到的流严格升序且无重复",
              f"n={len(stream_ids)}")
        final_stream = [e["id"] for e in fetch_page(proxy, page_start, 500)["events"]]
        check(stream_ids == final_stream,
              "并发翻页结果与事后的全量读取逐条一致（不漏）",
              f"stream={len(stream_ids)} final={len(final_stream)}")
        expected_live = {"WRITE_ACCEPTED", "WRITE_ACKED", "WRITE_TIMEOUT_UNCERTAIN",
                         "WRITE_RESOLVED"}
        stream_kinds = {e["kind"] for e in fetch_page(proxy, page_start, 500)["events"]}
        check(expected_live <= stream_kinds,
              "并发期间产生的各类结论变更都进入了可翻页的流",
              str(sorted(stream_kinds)))

        print("\n== 场景 7：代理重启后从旧游标继续，不重不漏 ==")
        # 场景 6b 之后重新取一次基线（重启前服务端的全量变更）。
        full = fetch_page(proxy, 0, 500)["events"]
        full_ids = [e["id"] for e in full]
        mid_cursor = full_ids[len(full_ids) // 2] if full_ids else start_cursor
        before_restart_latest = fetch_page(proxy, 0, 500)["latest"]
        proxy_proc.terminate()
        proxy_proc.wait(timeout=10)
        code, _ = http("GET", f"{proxy}/api/status")
        check(code != 200, "代理已停止")
        proxy_proc = spawn("proxy", ROOT / "proxy", proxy_env, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理使用同一 SQLite 卷重启成功")
        resumed = all_changes(proxy, mid_cursor, limit=2)
        expected = [e for e in full if e["id"] > mid_cursor]
        check([e["id"] for e in resumed] == [e["id"] for e in expected],
              "重启后从旧游标续看的结果与重启前完全一致",
              f"resumed={len(resumed)} expected={len(expected)}")
        check(fetch_page(proxy, 0, 500)["latest"] == before_restart_latest,
              "重启本身不产生变更事件，游标保持有效")
        first_after_restart = fetch_page(proxy, before_restart_latest, 10)
        check(first_after_restart["events"] == []
              and first_after_restart["latest"] == before_restart_latest,
              "重启后 after=重启前最新游标 立即追平")

        print("\n== 场景 7b：核实先确认 ACKED、迟到的读超时后到（交错） ==")
        fake = FakeBackend("C")
        # 停掉真实代理，换成一个以 C 为现役、指向受控后端的代理。
        proxy_proc.terminate()
        proxy_proc.wait(timeout=10)
        con = sqlite3.connect(proxy_db)
        con.execute(
            "INSERT OR REPLACE INTO nodes (name, base_url, created_at) VALUES (?, ?, ?)",
            ("C", f"http://127.0.0.1:{fake.port}", time.time()),
        )
        con.execute("UPDATE route SET active = 'C' WHERE id = 1")
        con.commit()
        con.close()
        c_port = ports["proxy"] + 10
        c_proc = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(c_port),
             "--app-dir", str(ROOT / "proxy")],
            env={**os.environ,
                 "NODE_A_NAME": "C", "NODE_B_NAME": "B",
                 "BACKEND_A_URL": f"http://127.0.0.1:{fake.port}",
                 "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
                 "PROXY_DB": str(proxy_db),
                 "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2",
                 "PROBE_TIMEOUT": "2"},
            stdout=open(logs / "proxy-c.log", "wb"), stderr=subprocess.STDOUT,
        )
        procs.append(c_proc)
        c_proxy = f"http://127.0.0.1:{c_port}"
        ok = wait_ready(c_proxy + "/")
        check(ok, "受控后端代理启动")
        if ok:
            check(status(c_proxy)["active"] == "C", "受控场景现役节点为 C")
            before_c = fetch_page(c_proxy, 0, 500)["latest"]
            poster = Poster(c_proxy, 0, "race-ack-vs-timeout")
            poster.start()
            switcher = Switcher(c_proxy, "B")
            # 等代理落库 PENDING，并抓住后端已落库、回执未归的窗口去核实。
            deadline = time.time() + 5
            wid = None
            while time.time() < deadline:
                _, st_now = http("GET", f"{c_proxy}/api/status")
                for w in st_now["writes"]:
                    if w["status"] == "PENDING":
                        wid = w["id"]
                        break
                if wid:
                    break
                time.sleep(0.05)
            check(wid is not None, "交错写入已进入 PENDING")
            switcher.start()
            # 后端已经落库（慢回执前 4 秒），向后端查询会返回 acked。
            code, ref = http("GET", f"{c_proxy}/api/writes/{wid}/refresh")
            check(code == 200 and ref["write"]["status"] == "ACKED",
                  "核实先确认 ACKED")
            poster.join(timeout=15)
            check(poster.code == 200 and poster.body.get("status") == "ACKED",
                  "迟到的读超时不再回退结论，响应据实返回 ACKED",
                  f"code={poster.code} status={poster.body.get('status')}")
            after_c = fetch_page(c_proxy, before_c, 500)
            kinds_c = [e["kind"] for e in after_c["events"]
                       if e["write_id"] == wid]
            check(kinds_c == ["WRITE_ACCEPTED", "WRITE_RESOLVED"],
                  "只记录受理与核实确认，没有虚假的超时事件", str(kinds_c))
            check(write_status(c_proxy, wid) == "ACKED", "最终结论为 ACKED")
            switcher.join(timeout=10)
            check(switcher.code == 200 and switcher.body.get("active") == "B",
                  "交错中切流仍正常")
        fake.close()
        c_proc.terminate()
        c_proc.wait(timeout=10)
        # 还原正式代理，供后续场景使用。
        con = sqlite3.connect(proxy_db)
        con.execute("DELETE FROM nodes WHERE name = 'C'")
        con.execute("UPDATE route SET active = 'A' WHERE id = 1")
        con.commit()
        con.close()
        proxy_proc = spawn("proxy", ROOT / "proxy", proxy_env, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理恢复")

        print("\n== 场景 8：事件写入失败与状态变更同事务回滚 ==")
        latest0 = fetch_page(proxy, 0, 500)["latest"]
        install_change_trigger(proxy_db, "WRITE_ACCEPTED")
        try:
            code, failed = http("POST", f"{proxy}/api/writes?delay=0",
                                {"id": "atomic-accept-fail", "payload": "x"})
        finally:
            clear_change_triggers(proxy_db)
        check(code == 500, "受理事件写入失败 -> 请求失败", f"code={code}")
        _, s_now = http("GET", f"{proxy}/api/status")
        check(not any(w["id"] == "atomic-accept-fail" for w in s_now["writes"]),
              "失败的受理没有留下 writes 行（状态与事件一起回滚）")
        check(fetch_page(proxy, 0, 500)["latest"] == latest0,
              "失败的受理没有留下事件")
        _, rec_a2 = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b2 = http("GET", f"{proxy}/api/nodes/B/records")
        check(not any(r["id"] == "atomic-accept-fail"
                      for r in rec_a2["records"] + rec_b2["records"]),
              "事务失败后也没有投递到任何后端")

        active_before = status(proxy)["active"]
        other = "B" if active_before == "A" else "A"
        latest1 = fetch_page(proxy, 0, 500)["latest"]
        install_change_trigger(proxy_db, "SWITCH_OK")
        try:
            code, failed = http("POST", f"{proxy}/api/switch", {"target": other})
        finally:
            clear_change_triggers(proxy_db)
        check(code == 500, "SWITCH_OK 事件写入失败 -> 切流请求失败", f"code={code}")
        check(status(proxy)["active"] == active_before,
              "失败的切流未改变路由（路由与事件一起回滚）")
        check(fetch_page(proxy, 0, 500)["latest"] == latest1,
              "失败的切流没有留下事件")

        latest2 = fetch_page(proxy, 0, 500)["latest"]
        install_change_trigger(proxy_db, "WRITE_ACKED")
        try:
            code, failed = http("POST", f"{proxy}/api/writes?delay=0",
                                {"id": "atomic-ack-fail", "payload": "y"})
        finally:
            clear_change_triggers(proxy_db)
        check(code == 500, "结论事件写入失败 -> 回执请求失败", f"code={code}")
        row_state = write_status(proxy, "atomic-ack-fail")
        check(row_state == "PENDING",
              "结论变更与事件一起回滚，写入仍是 PENDING", f"state={row_state}")
        after_fail = fetch_page(proxy, latest2, 100)
        check([e["kind"] for e in after_fail["events"]] == ["WRITE_ACCEPTED"],
              "失败结论没有留下虚假的 ACKED 事件",
              str([e["kind"] for e in after_fail["events"]]))
        code, ref3 = http("GET", f"{proxy}/api/writes/atomic-ack-fail/refresh")
        check(code == 200 and ref3["write"]["status"] == "ACKED",
              "移除故障后核实可补齐真实结论")
        kinds3 = [e["kind"] for e in fetch_page(proxy, latest2, 100)["events"]]
        check(kinds3 == ["WRITE_ACCEPTED", "WRITE_RESOLVED"],
              "补齐结论时恰好新增一条变更", str(kinds3))

        print("\n== 场景 9：全量事件与真实状态终检 ==")
        final = all_changes(proxy, 0)
        ids = [e["id"] for e in final]
        check(ids == sorted(set(ids)), "全量变更 ID 严格升序无重复")
        check(all(e["kind"] in {"SWITCH_OK", "WRITE_ACCEPTED", "WRITE_ACKED",
                                "WRITE_TIMEOUT_UNCERTAIN", "WRITE_RESOLVED",
                                "WRITE_FAILED"} for e in final),
              "变更记录只包含业务变更类型")
        check(all((e["kind"] == "SWITCH_OK" and e["node"])
                  or (e["kind"] != "SWITCH_OK" and e["write_id"] and e["node"])
                  for e in final),
              "每条变更都有节点；写入类变更都有写入 ID")
        check(all((e["kind"] == "SWITCH_OK" and e["write_id"] is None
                   and e["state"] == e["node"])
                  or (e["kind"] != "SWITCH_OK" and e["state"])
                  for e in final),
              "切流记录新状态（目标节点），写入类记录新状态")

    finally:
        clear_change_triggers(proxy_db)
        print("\n== 清理进程 ==")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 变更记录自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
