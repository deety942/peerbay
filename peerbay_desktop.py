#!/usr/bin/env python3
import json
import queue
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk

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

        defaults = {
            "shared_dir": str(BASE_DIR / "shared"),
            "index": str(BASE_DIR / "peer_index.json"),
            "server": "https://p2p-archive-index.onrender.com",
            "peer_url": "http://127.0.0.1:9090",
            "token": "",
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
        self._field(frame, 5, "Read Token", "token", secret=True)
        self._field(frame, 6, "Peer Host", "host")
        self._field(frame, 7, "Peer Port", "port")

        btns = ttk.Frame(frame)
        btns.grid(row=8, column=0, columnspan=4, sticky="ew", pady=(12, 6))
        btns.columnconfigure((0, 1, 2, 3, 4), weight=1)

        ttk.Button(btns, text="Save Config", command=self.save_config).grid(row=0, column=0, sticky="ew", padx=4)
        ttk.Button(btns, text="Scan Folder", command=self.scan_folder).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(btns, text="Start Peer Server", command=self.start_server).grid(row=0, column=2, sticky="ew", padx=4)
        ttk.Button(btns, text="Stop Peer Server", command=self.stop_server).grid(row=0, column=3, sticky="ew", padx=4)
        ttk.Button(btns, text="Publish Availability", command=self.publish).grid(row=0, column=4, sticky="ew", padx=4)

        ttk.Separator(frame).grid(row=9, column=0, columnspan=4, sticky="ew", pady=8)

        self._field(frame, 10, "Download CID", "download_cid")
        self._field(frame, 11, "Download Folder", "download_dir", browse="dir")

        ttk.Button(frame, text="Download CID", command=self.download_cid).grid(row=12, column=0, columnspan=4, sticky="ew", pady=(6, 10))

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(frame, textvariable=self.status_var).grid(row=13, column=0, columnspan=4, sticky="w", pady=(0, 6))

        self.log = tk.Text(frame, height=20, wrap=tk.WORD)
        self.log.grid(row=14, column=0, columnspan=4, sticky="nsew")
        frame.rowconfigure(14, weight=1)
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
            "--token",
            self.vars["token"].get(),
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
            "--token",
            self.vars["token"].get(),
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


def main() -> None:
    if not PEER_SCRIPT.exists():
        raise FileNotFoundError(f"Missing peer script: {PEER_SCRIPT}")
    root = tk.Tk()
    app = PeerBayDesktop(root)

    def on_close() -> None:
        app.stop_server()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
