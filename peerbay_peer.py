#!/usr/bin/env python3
import argparse
import base64
import hashlib
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


def basic_auth_header(username: str, password: str) -> str:
    raw = f"{username}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def scan_shared(shared_dir: Path) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    for p in shared_dir.rglob("*"):
        if not p.is_file():
            continue
        cid = file_sha256(p)
        stat = p.stat()
        rel = str(p.relative_to(shared_dir)).replace("\\", "/")
        files.append(
            {
                "cid": cid,
                "path": str(p.resolve()),
                "rel_path": rel,
                "file_name": p.name,
                "file_size": stat.st_size,
            }
        )
    return files


def write_index(index_path: Path, files: List[Dict[str, Any]]) -> None:
    payload = {"files": files}
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_index(index_path: Path) -> Dict[str, Dict[str, Any]]:
    if not index_path.exists():
        return {}
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    out: Dict[str, Dict[str, Any]] = {}
    for item in payload.get("files", []):
        cid = item.get("cid")
        if isinstance(cid, str) and cid:
            out[cid] = item
    return out


class PeerHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, index_path: Path, **kwargs):
        self.index_path = index_path
        super().__init__(*args, **kwargs)

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json({"ok": True, "service": "peerbay-peer"})
            return
        if parsed.path.startswith("/file/"):
            cid = parsed.path.split("/", 2)[2].strip().lower()
            idx = load_index(self.index_path)
            item = idx.get(cid)
            if not item:
                self._send_json({"error": "not found"}, status=404)
                return
            p = Path(item["path"]).resolve()
            if not p.exists() or not p.is_file():
                self._send_json({"error": "missing"}, status=404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(p.stat().st_size))
            self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{quote(p.name)}")
            self.end_headers()
            with p.open("rb") as fh:
                while True:
                    chunk = fh.read(64 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            return
        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def make_handler(index_path: Path):
    def handler(*args, **kwargs):
        return PeerHandler(*args, index_path=index_path, **kwargs)

    return handler


def cmd_serve(args: argparse.Namespace) -> None:
    index_path = Path(args.index).resolve()
    if not index_path.exists():
        raise FileNotFoundError(f"Index file not found: {index_path}. Run 'scan' first.")
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(index_path))
    print(json.dumps({"status": "serving", "host": args.host, "port": args.port, "index": str(index_path)}, indent=2))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


def cmd_scan(args: argparse.Namespace) -> None:
    shared_dir = Path(args.shared_dir).expanduser().resolve()
    if not shared_dir.exists() or not shared_dir.is_dir():
        raise FileNotFoundError(f"Shared directory not found: {shared_dir}")
    files = scan_shared(shared_dir)
    write_index(Path(args.index), files)
    print(json.dumps({"shared_dir": str(shared_dir), "indexed": len(files), "index": str(Path(args.index).resolve())}, indent=2))


def cmd_publish(args: argparse.Namespace) -> None:
    index = load_index(Path(args.index))
    if not index:
        raise ValueError("No indexed files. Run 'scan' first.")

    published = 0
    for cid, item in index.items():
        payload = {
            "cid": cid,
            "peer_url": args.peer_url.rstrip("/"),
            "file_name": item.get("file_name"),
            "file_size": item.get("file_size"),
        }
        req = Request(
            f"{args.server.rstrip('/')}/api/p2p/publish",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": basic_auth_header(args.username, args.password),
            },
            method="POST",
        )
        with urlopen(req, timeout=args.timeout) as resp:
            _ = json.loads(resp.read().decode("utf-8"))
        published += 1
    print(json.dumps({"published": published, "server": args.server, "peer_url": args.peer_url}, indent=2))


def cmd_download(args: argparse.Namespace) -> None:
    req = Request(
        f"{args.server.rstrip('/')}/api/p2p/providers?cid={args.cid}",
        headers={"Authorization": basic_auth_header(args.username, args.password), "Accept": "application/json"},
    )
    with urlopen(req, timeout=args.timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    providers = payload.get("providers", [])
    if not providers:
        raise ValueError("No providers found for CID")

    dest_dir = Path(args.dest_dir).expanduser().resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    last_error: Optional[str] = None
    for p in providers:
        peer_url = str(p.get("peer_url", "")).rstrip("/")
        if not peer_url:
            continue
        name = p.get("file_name") or f"{args.cid}.bin"
        dest = dest_dir / name
        try:
            req_file = Request(f"{peer_url}/file/{args.cid}", headers={"Accept": "application/octet-stream"})
            with urlopen(req_file, timeout=args.timeout) as resp, dest.open("wb") as out:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            got = file_sha256(dest)
            if got != args.cid:
                dest.unlink(missing_ok=True)
                raise ValueError("hash mismatch")
            print(json.dumps({"ok": True, "peer_url": peer_url, "saved_to": str(dest)}, indent=2))
            return
        except (URLError, ValueError, OSError) as exc:
            last_error = str(exc)
            continue

    raise RuntimeError(f"Download failed from all providers: {last_error or 'unknown error'}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="peerBay local peer client")
    parser.add_argument("--index", default="peer_index.json", help="Path to local peer index JSON")

    sub = parser.add_subparsers(dest="command", required=True)

    scan = sub.add_parser("scan", help="Scan local shared directory and build index")
    scan.add_argument("--shared-dir", required=True)
    scan.set_defaults(func=cmd_scan)

    serve = sub.add_parser("serve", help="Serve indexed files for direct P2P downloads")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=9090)
    serve.set_defaults(func=cmd_serve)

    publish = sub.add_parser("publish", help="Publish available files to peerBay index server")
    publish.add_argument("--server", required=True, help="peerBay server URL")
    publish.add_argument("--peer-url", required=True, help="Public URL of this peer service")
    publish.add_argument("--username", required=True, help="Account username")
    publish.add_argument("--password", required=True, help="Account password")
    publish.add_argument("--timeout", type=int, default=20)
    publish.set_defaults(func=cmd_publish)

    download = sub.add_parser("download", help="Download a CID directly from listed peers")
    download.add_argument("--server", required=True)
    download.add_argument("--username", required=True, help="Account username")
    download.add_argument("--password", required=True, help="Account password")
    download.add_argument("--cid", required=True)
    download.add_argument("--dest-dir", default="./downloads")
    download.add_argument("--timeout", type=int, default=25)
    download.set_defaults(func=cmd_download)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
