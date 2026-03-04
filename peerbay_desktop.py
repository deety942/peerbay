#!/usr/bin/env python3
import base64
import json
import queue
import re
import shutil
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

APP_TITLE = "peerBay Desktop"
CONFIG_PATH = Path.home() / ".peerbay_desktop.json"
BASE_DIR = Path(__file__).resolve().parent
PEER_SCRIPT = BASE_DIR / "peerbay_peer.py"


class PeerBayDesktop:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("980x760")
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.server_proc: subprocess.Popen[str] | None = None
        self.tunnel_proc: subprocess.Popen[str] | None = None

        defaults = {
            "shared_dir": str(BASE_DIR / "shared"),
            "index": str(BASE_DIR / "peer_index.json"),
            "server": "https://p2p-archive-index.onrender.com",
            "peer_url": "http://127.0.0.1:9090",
            "auth_username": "",
            "auth_password": "",
            "browse_path": "",
            "host": "0.0.0.0",
            "port": "9090",
            "download_cid": "",
            "download_dir": str(BASE_DIR / "downloads"),
        }
        defaults.update(self._load_config())

        self.vars = {k: tk.StringVar(value=v) for k, v in defaults.items()}

        self._build_ui()
        self._poll_logs()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        title = ttk.Label(frame, text=APP_TITLE, font=("Helvetica", 20, "bold"))
        title.grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 10))

        self._field(frame, 1, "Shared Folder", "shared_dir", browse="dir")
        self._field(frame, 2, "Index File", "index", browse="file_save")
        self._field(frame, 3, "Index Server URL", "server")
        self._field(frame, 4, "Your Peer URL", "peer_url")
        self._field(frame, 5, "Peer Host", "host")
        self._field(frame, 6, "Peer Port", "port")
        self._field(frame, 7, "Account Username", "auth_username")
        self._field(frame, 8, "Account Password", "auth_password", secret=True)

        btns = ttk.Frame(frame)
        btns.grid(row=9, column=0, columnspan=4, sticky="ew", pady=(12, 6))
        btns.columnconfigure((0, 1, 2, 3, 4, 5), weight=1)

        ttk.Button(btns, text="Save Config", command=self.save_config).grid(row=0, column=0, sticky="ew", padx=4)
        ttk.Button(btns, text="Scan Folder", command=self.scan_folder).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(btns, text="Start Peer Server", command=self.start_server).grid(row=0, column=2, sticky="ew", padx=4)
        ttk.Button(btns, text="Stop Peer Server", command=self.stop_server).grid(row=0, column=3, sticky="ew", padx=4)
        ttk.Button(btns, text="Publish Availability", command=self.publish).grid(row=0, column=4, sticky="ew", padx=4)
        ttk.Button(btns, text="Start Tunnel", command=self.start_tunnel).grid(row=0, column=5, sticky="ew", padx=4)
        ttk.Button(btns, text="Sign Up", command=self.signup).grid(row=1, column=1, sticky="ew", padx=4, pady=(6, 0))
        ttk.Button(btns, text="Login", command=self.login).grid(row=1, column=2, sticky="ew", padx=4, pady=(6, 0))
        ttk.Button(btns, text="Stop Tunnel", command=self.stop_tunnel).grid(row=1, column=3, sticky="ew", padx=4, pady=(6, 0))

        ttk.Separator(frame).grid(row=10, column=0, columnspan=4, sticky="ew", pady=8)

        self._field(frame, 11, "Browse Path", "browse_path")
        browse_btns = ttk.Frame(frame)
        browse_btns.grid(row=12, column=0, columnspan=4, sticky="ew", pady=(0, 6))
        browse_btns.columnconfigure((0, 1), weight=1)
        ttk.Button(browse_btns, text="Browse Path", command=self.browse_path).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(browse_btns, text="Search Files", command=self.search_files).grid(row=0, column=1, sticky="ew", padx=(4, 0))

        cols = ("name", "kind", "size", "user", "cid")
        self.browse_table = ttk.Treeview(frame, columns=cols, show="headings", height=9)
        self.browse_table.heading("name", text="Name")
        self.browse_table.heading("kind", text="Type")
        self.browse_table.heading("size", text="Size")
        self.browse_table.heading("user", text="User")
        self.browse_table.heading("cid", text="CID")
        self.browse_table.column("name", width=280, anchor="w")
        self.browse_table.column("kind", width=70, anchor="center")
        self.browse_table.column("size", width=90, anchor="e")
        self.browse_table.column("user", width=120, anchor="w")
        self.browse_table.column("cid", width=320, anchor="w")
        self.browse_table.grid(row=13, column=0, columnspan=4, sticky="nsew")
        self.browse_table.bind("<Double-1>", self._on_browse_double_click)

        self._field(frame, 14, "Download CID", "download_cid")
        self._field(frame, 15, "Download Folder", "download_dir", browse="dir")

        ttk.Button(frame, text="Download CID", command=self.download_cid).grid(row=16, column=0, columnspan=4, sticky="ew", pady=(6, 10))

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(frame, textvariable=self.status_var).grid(row=17, column=0, columnspan=4, sticky="w", pady=(0, 6))

        self.log = tk.Text(frame, height=20, wrap=tk.WORD)
        self.log.grid(row=18, column=0, columnspan=4, sticky="nsew")
        frame.rowconfigure(18, weight=1)
        frame.columnconfigure(1, weight=1)

    def _field(self, parent: ttk.Frame, row: int, label: str, key: str, browse: str | None = None, secret: bool = False) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=(0, 8), pady=3)
        show = "*" if secret else ""
        entry = ttk.Entry(parent, textvariable=self.vars[key], show=show)
        entry.grid(row=row, column=1, columnspan=2, sticky="ew", pady=3)
        parent.columnconfigure(1, weight=1)

        if browse == "dir":
            ttk.Button(parent, text="Browse", command=lambda k=key: self._pick_dir(k)).grid(row=row, column=3, sticky="ew", padx=(8, 0), pady=3)
        elif browse == "file_save":
            ttk.Button(parent, text="Browse", command=lambda k=key: self._pick_file(k)).grid(row=row, column=3, sticky="ew", padx=(8, 0), pady=3)

    def _pick_dir(self, key: str) -> None:
        p = filedialog.askdirectory(initialdir=self.vars[key].get() or str(BASE_DIR))
        if p:
            self.vars[key].set(p)

    def _pick_file(self, key: str) -> None:
        p = filedialog.asksaveasfilename(initialfile="peer_index.json", initialdir=str(BASE_DIR))
        if p:
            self.vars[key].set(p)

    def _load_config(self) -> dict:
        if not CONFIG_PATH.exists():
            return {}
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_config(self) -> None:
        payload = {k: v.get() for k, v in self.vars.items()}
        CONFIG_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self._set_status("Config saved")

    def _set_status(self, msg: str) -> None:
        self.status_var.set(msg)
        self._log(msg)

    def _log(self, msg: str) -> None:
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)

    def _poll_logs(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self._log(msg)
        except queue.Empty:
            pass
        self.root.after(120, self._poll_logs)

    def _run_cmd_async(self, args: list[str], success_msg: str) -> None:
        def runner() -> None:
            try:
                self.log_queue.put("$ " + " ".join(args))
                proc = subprocess.run(args, capture_output=True, text=True, check=False)
                if proc.stdout.strip():
                    self.log_queue.put(proc.stdout.strip())
                if proc.returncode != 0:
                    self.log_queue.put(proc.stderr.strip() or f"Command failed ({proc.returncode})")
                    self.status_var.set("Error")
                    return
                self.status_var.set(success_msg)
            except Exception as exc:
                self.log_queue.put(str(exc))
                self.status_var.set("Error")

        threading.Thread(target=runner, daemon=True).start()

    def _auth_headers(self) -> Dict[str, str]:
        username = self.vars["auth_username"].get().strip()
        password = self.vars["auth_password"].get()
        if not username or not password:
            return {}
        raw = f"{username}:{password}".encode("utf-8")
        encoded = base64.b64encode(raw).decode("ascii")
        return {"Authorization": f"Basic {encoded}"}

    def _http_json(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.vars["server"].get().rstrip("/") + path
        body = None
        headers = {"Accept": "application/json"}
        headers.update(self._auth_headers())
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = Request(url, data=body, headers=headers, method=method)
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _run_http_async(self, label: str, fn, on_success=None) -> None:
        def runner() -> None:
            try:
                self.log_queue.put(label)
                data = fn()
                if on_success:
                    self.root.after(0, lambda d=data: on_success(d))
                self.root.after(0, lambda: self.status_var.set("Done"))
            except (HTTPError, URLError, ValueError) as exc:
                msg = str(exc)
                self.log_queue.put(msg)
                self.root.after(0, lambda: self.status_var.set("Error"))
            except Exception as exc:  # noqa: BLE001
                self.log_queue.put(str(exc))
                self.root.after(0, lambda: self.status_var.set("Error"))

        threading.Thread(target=runner, daemon=True).start()

    def _set_auth_ok(self, data: Dict[str, Any]) -> None:
        username = str(data.get("username", "")).strip()
        if username:
            self.vars["auth_username"].set(username)
            self.save_config()
            self._set_status(f"Authenticated as {username}")

    def signup(self) -> None:
        username = self.vars["auth_username"].get().strip()
        password = self.vars["auth_password"].get()
        if len(username) < 3 or len(password) < 10:
            self._set_status("Signup needs username >=3 and password >=10")
            return

        self._run_http_async(
            "Signing up...",
            lambda: self._http_json("POST", "/api/signup", {"username": username, "password": password}),
            self._set_auth_ok,
        )

    def login(self) -> None:
        username = self.vars["auth_username"].get().strip()
        password = self.vars["auth_password"].get()
        if not username or not password:
            self._set_status("Login needs username and password")
            return
        self._run_http_async(
            "Logging in...",
            lambda: self._http_json("POST", "/api/login", {"username": username, "password": password}),
            self._set_auth_ok,
        )

    def _fmt_bytes(self, size: Any) -> str:
        try:
            n = int(size or 0)
        except Exception:
            return "-"
        if n <= 0:
            return "-"
        units = ["B", "KB", "MB", "GB", "TB"]
        v = float(n)
        idx = 0
        while v >= 1024 and idx < len(units) - 1:
            v /= 1024
            idx += 1
        return f"{v:.1f} {units[idx]}" if idx else f"{int(v)} B"

    def _render_browse(self, data: Dict[str, Any]) -> None:
        for row in self.browse_table.get_children():
            self.browse_table.delete(row)
        for item in data.get("items", []):
            kind = item.get("kind", "")
            if kind == "dir":
                name = str(item.get("name", "")) + "/"
                size = "-"
                user = "-"
                cid = "-"
            else:
                name = item.get("name", "")
                size = self._fmt_bytes(item.get("file_size"))
                user = item.get("source_node", "")
                cid = item.get("cid", "")
            self.browse_table.insert("", tk.END, values=(name, kind or "file", size, user, cid))
        self._set_status(f"Loaded {data.get('count', 0)} items")

    def browse_path(self) -> None:
        path = self.vars["browse_path"].get().strip()
        query = f"?{urlencode({'path': path})}" if path else ""
        self._run_http_async("Browsing...", lambda: self._http_json("GET", f"/api/browse{query}"), self._render_browse)

    def _render_search(self, data: Dict[str, Any]) -> None:
        for row in self.browse_table.get_children():
            self.browse_table.delete(row)
        for item in data.get("entries", []):
            name = item.get("rel_path") or item.get("file_name") or "(unnamed)"
            size = self._fmt_bytes(item.get("file_size"))
            user = item.get("source_node", "")
            cid = item.get("cid", "")
            self.browse_table.insert("", tk.END, values=(name, "file", size, user, cid))
        self._set_status(f"Search returned {data.get('count', 0)} files")

    def search_files(self) -> None:
        q = self.vars["browse_path"].get().strip()
        query = f"?{urlencode({'query': q, 'limit': '500'})}" if q else "?limit=500"
        self._run_http_async("Searching...", lambda: self._http_json("GET", f"/api/entries{query}"), self._render_search)

    def _on_browse_double_click(self, _event: tk.Event) -> None:
        sel = self.browse_table.selection()
        if not sel:
            return
        vals = self.browse_table.item(sel[0]).get("values", [])
        if len(vals) < 5:
            return
        kind = str(vals[1])
        name = str(vals[0])
        cid = str(vals[4])
        if kind == "dir":
            base = self.vars["browse_path"].get().strip().strip("/")
            part = name.rstrip("/")
            next_path = f"{base}/{part}" if base else part
            self.vars["browse_path"].set(next_path)
            self.browse_path()
            return
        if cid and cid != "-":
            self.vars["download_cid"].set(cid)
            self._set_status("CID loaded from selected row; click Download CID to fetch")

    def scan_folder(self) -> None:
        self.save_config()
        args = [
            sys.executable,
            str(PEER_SCRIPT),
            "--index",
            self.vars["index"].get(),
            "scan",
            "--shared-dir",
            self.vars["shared_dir"].get(),
        ]
        self._run_cmd_async(args, "Scan complete")

    def publish(self) -> None:
        self.save_config()
        args = [
            sys.executable,
            str(PEER_SCRIPT),
            "--index",
            self.vars["index"].get(),
            "publish",
            "--server",
            self.vars["server"].get(),
            "--peer-url",
            self.vars["peer_url"].get(),
            "--username",
            self.vars["auth_username"].get(),
            "--password",
            self.vars["auth_password"].get(),
        ]
        self._run_cmd_async(args, "Publish complete")

    def download_cid(self) -> None:
        self.save_config()
        cid = self.vars["download_cid"].get().strip()
        if not cid:
            self._set_status("Download CID is required")
            return
        args = [
            sys.executable,
            str(PEER_SCRIPT),
            "--index",
            self.vars["index"].get(),
            "download",
            "--server",
            self.vars["server"].get(),
            "--username",
            self.vars["auth_username"].get(),
            "--password",
            self.vars["auth_password"].get(),
            "--cid",
            cid,
            "--dest-dir",
            self.vars["download_dir"].get(),
        ]
        self._run_cmd_async(args, "Download complete")

    def start_server(self) -> None:
        self.save_config()
        if self.server_proc and self.server_proc.poll() is None:
            self._set_status("Peer server already running")
            return
        args = [
            sys.executable,
            str(PEER_SCRIPT),
            "--index",
            self.vars["index"].get(),
            "serve",
            "--host",
            self.vars["host"].get(),
            "--port",
            self.vars["port"].get(),
        ]
        self.server_proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        def tail() -> None:
            assert self.server_proc is not None
            if self.server_proc.stdout is None:
                return
            for line in self.server_proc.stdout:
                self.log_queue.put(line.rstrip())

        threading.Thread(target=tail, daemon=True).start()
        self._set_status("Peer server started")

    def stop_server(self) -> None:
        if not self.server_proc or self.server_proc.poll() is not None:
            self._set_status("Peer server is not running")
            return
        self.server_proc.terminate()
        self._set_status("Peer server stopped")

    def start_tunnel(self) -> None:
        if self.tunnel_proc and self.tunnel_proc.poll() is None:
            self._set_status("Tunnel already running")
            return
        if not shutil.which("cloudflared"):
            self._set_status("cloudflared not installed. Run: brew install cloudflared")
            return
        port = self.vars["port"].get().strip() or "9090"
        args = ["cloudflared", "tunnel", "--url", f"http://localhost:{port}"]
        self.tunnel_proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        def tail() -> None:
            assert self.tunnel_proc is not None
            if self.tunnel_proc.stdout is None:
                return
            for line in self.tunnel_proc.stdout:
                s = line.rstrip()
                self.log_queue.put(s)
                m = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", s)
                if m:
                    url = m.group(0)
                    self.root.after(0, lambda u=url: self.vars["peer_url"].set(u))
                    self.root.after(0, lambda: self.status_var.set("Tunnel started; peer URL auto-filled"))

        threading.Thread(target=tail, daemon=True).start()
        self._set_status("Starting tunnel...")

    def stop_tunnel(self) -> None:
        if not self.tunnel_proc or self.tunnel_proc.poll() is not None:
            self._set_status("Tunnel is not running")
            return
        self.tunnel_proc.terminate()
        self._set_status("Tunnel stopped")


def main() -> None:
    if not PEER_SCRIPT.exists():
        raise FileNotFoundError(f"Missing peer script: {PEER_SCRIPT}")
    root = tk.Tk()
    app = PeerBayDesktop(root)

    def on_close() -> None:
        app.stop_server()
        app.stop_tunnel()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
