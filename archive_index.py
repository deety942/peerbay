#!/usr/bin/env python3
import argparse
import base64
import binascii
import datetime as dt
import hashlib
import hmac
import json
import mimetypes
import os
import secrets
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

SCHEMA = """
CREATE TABLE IF NOT EXISTS entries (
  id TEXT PRIMARY KEY,
  cid TEXT NOT NULL,
  title TEXT,
  description TEXT,
  tags TEXT,
  file_name TEXT,
  file_size INTEGER,
  file_mtime REAL,
  local_path TEXT,
  source_node TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  signature TEXT,
  meta_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_entries_cid ON entries(cid);
CREATE INDEX IF NOT EXISTS idx_entries_updated_at ON entries(updated_at);
CREATE TABLE IF NOT EXISTS peers (
  url TEXT PRIMARY KEY,
  added_at TEXT NOT NULL,
  last_seen_at TEXT,
  last_sync_at TEXT,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  password_salt TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  last_login_at TEXT
);
CREATE TABLE IF NOT EXISTS api_tokens (
  token_hash TEXT PRIMARY KEY,
  token_prefix TEXT NOT NULL,
  role TEXT NOT NULL,
  username TEXT NOT NULL,
  created_at TEXT NOT NULL,
  revoked_at TEXT,
  last_used_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_tokens_role ON api_tokens(role);
CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON api_tokens(username);
CREATE TABLE IF NOT EXISTS p2p_sources (
  cid TEXT NOT NULL,
  username TEXT NOT NULL,
  peer_url TEXT NOT NULL,
  file_name TEXT,
  file_size INTEGER,
  announced_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  PRIMARY KEY (cid, username, peer_url)
);
CREATE INDEX IF NOT EXISTS idx_p2p_sources_cid ON p2p_sources(cid);
"""

SIGN_FIELDS = [
    "id",
    "cid",
    "title",
    "description",
    "tags",
    "file_name",
    "file_size",
    "file_mtime",
    "source_node",
    "created_at",
    "updated_at",
]

MAX_REQUEST_BODY = 128 * 1024

INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>peerBay</title>
  <style>
    :root {
      --bg: #161b20;
      --bg-soft: #1b2026;
      --ink: #f3f6f8;
      --muted: #a7b0b8;
      --line: #c8d0d6;
      --accent: #91d6ff;
      --warn: #ff8f8f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background: linear-gradient(90deg, #11161b 0%, #1a2026 55%, #11161b 100%);
      font-family: "Courier New", "Lucida Console", monospace;
    }
    .wrap {
      width: min(980px, 94vw);
      margin: 22px auto 28px;
      padding: 0 10px;
    }
    h1 {
      margin: 0 0 14px;
      text-align: center;
      font-size: 50px;
      letter-spacing: 0.02em;
      font-weight: 700;
    }
    .subtitle {
      text-align: center;
      color: var(--muted);
      margin: -8px 0 18px;
      font-size: 17px;
    }
    .search-box {
      display: grid;
      grid-template-columns: 1fr auto auto auto;
      gap: 8px;
      margin: 0 auto 14px;
      width: min(760px, 100%);
    }
    input {
      border: 1px solid var(--line);
      border-radius: 4px;
      background: #232a33;
      color: var(--ink);
      padding: 10px 11px;
      font-size: 15px;
      font-family: inherit;
    }
    input::placeholder { color: #9ba5ae; }
    button {
      border: 1px solid var(--line);
      border-radius: 4px;
      background: #21272e;
      color: var(--ink);
      padding: 10px 12px;
      font-size: 14px;
      font-family: inherit;
      cursor: pointer;
      white-space: nowrap;
    }
    button:hover {
      background: #2a313a;
    }
    .panel {
      border: 1px solid rgba(200, 208, 214, 0.32);
      background: rgba(22, 28, 35, 0.5);
      padding: 12px;
      margin-bottom: 14px;
      border-radius: 4px;
    }
    .panel-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      align-items: end;
    }
    .field label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin: 0 0 5px;
    }
    .field input {
      width: 100%;
    }
    .actions {
      margin-top: 10px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .meta {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }
    .status {
      min-height: 20px;
      margin: 8px 0 10px;
      font-size: 13px;
      color: var(--muted);
    }
    .status.err { color: var(--warn); }
    .table-wrap {
      border-top: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 19px;
      line-height: 1.25;
    }
    th, td {
      border-bottom: 1px solid rgba(200, 208, 214, 0.95);
      text-align: left;
      padding: 8px 10px;
      vertical-align: top;
      font-size: 17px;
      font-weight: 700;
    }
    th {
      color: var(--ink);
      text-decoration: underline;
      text-underline-offset: 0.12em;
    }
    td code {
      font-size: 17px;
      color: #bed6e8;
      word-break: break-all;
      display: inline-block;
      max-width: 310px;
    }
    td:nth-child(2),
    td:nth-child(3),
    td:nth-child(4),
    td:nth-child(5) {
      font-size: 17px;
    }
    a {
      color: var(--ink);
      text-decoration: underline;
      text-underline-offset: 0.08em;
    }
    a:hover {
      color: var(--accent);
    }
    .footer {
      margin-top: 20px;
      text-align: center;
      color: var(--muted);
      font-size: 15px;
    }
    @media (max-width: 1100px) {
      h1 { font-size: 38px; }
      table { font-size: 18px; }
      th, td { font-size: 15px; padding: 7px 8px; }
      td:nth-child(2),
      td:nth-child(3),
      td:nth-child(4),
      td:nth-child(5) { font-size: 14px; }
      td code { font-size: 12px; max-width: 180px; }
      .panel-grid {
        grid-template-columns: 1fr;
      }
      .search-box {
        grid-template-columns: 1fr;
      }
      .actions button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>peerBay</h1>
    <p id="pathLabel" class="subtitle">Index of /shares/</p>

    <div class="search-box">
      <input id="searchQuery" placeholder="Type here to search this directory" />
      <button id="searchBtn">Search</button>
      <button id="rootBtn">Root</button>
      <button id="reloadBtn">Reload</button>
    </div>

    <section class="panel">
      <div class="panel-grid">
        <div class="field">
          <label for="accessToken">Access Token</label>
          <input id="accessToken" placeholder="optional token for private nodes" />
        </div>
        <div class="field">
          <label for="username">Username</label>
          <input id="username" placeholder="dylan" />
        </div>
        <div class="field">
          <label for="sharedDir">Shared Folder</label>
          <input id="sharedDir" placeholder="/absolute/path/to/share" />
        </div>
      </div>
      <div class="actions">
        <button id="saveProfileBtn">Save Profile</button>
        <button id="rescanBtn">Rescan Folder</button>
      </div>
      <hr style="border:0;border-top:1px solid rgba(200,208,214,.25);margin:12px 0;" />
      <div class="panel-grid">
        <div class="field">
          <label for="uploadInput">Upload Files/Folder</label>
          <input id="uploadInput" type="file" multiple webkitdirectory directory />
        </div>
      </div>
      <div class="actions">
        <button id="uploadBtn">Upload Selected</button>
      </div>
      <hr style="border:0;border-top:1px solid rgba(200,208,214,.25);margin:12px 0;" />
      <div class="panel-grid">
        <div class="field">
          <label for="authUsername">Signup/Login User</label>
          <input id="authUsername" placeholder="newuser" />
        </div>
        <div class="field">
          <label for="authPassword">Password</label>
          <input id="authPassword" type="password" placeholder="at least 10 characters" />
        </div>
        <div class="field">
          <label for="issuedToken">Issued Read Token</label>
          <input id="issuedToken" readonly placeholder="generated after signup/login" />
        </div>
      </div>
      <div class="actions">
        <button id="signupBtn">Sign Up + Generate Tokens</button>
        <button id="loginBtn">Login + Rotate Tokens</button>
      </div>
      <p id="peerInfo" class="meta">Known peers: 0</p>
    </section>

    <div id="status" class="status">Ready.</div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>File Name</th>
            <th>File Size</th>
            <th>Date</th>
            <th>User</th>
            <th>Type</th>
            <th>Tags</th>
            <th>Hash</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
    </div>

    <p class="footer">peerBay is a distributed peer archive index.</p>
  </div>

  <script>
    const statusEl = document.getElementById("status");
    const rowsEl = document.getElementById("rows");
    const tokenEl = document.getElementById("accessToken");
    const pathLabelEl = document.getElementById("pathLabel");
    const issuedTokenEl = document.getElementById("issuedToken");
    const uploadInputEl = document.getElementById("uploadInput");
    let currentBrowsePath = "";
    let currentMode = "browse";

    function setStatus(msg, isErr = false) {
      statusEl.textContent = msg;
      statusEl.className = isErr ? "status err" : "status";
    }

    function esc(v) {
      const s = (v ?? "").toString();
      return s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    }

    function tokenValue() {
      return (tokenEl.value || "").trim();
    }

    function authHeaders(isJson = false) {
      const headers = {};
      if (isJson) headers["Content-Type"] = "application/json";
      const token = tokenValue();
      if (token) headers["Authorization"] = "Bearer " + token;
      return headers;
    }

    function saveToken() {
      localStorage.setItem("peerBayAccessToken", tokenValue());
    }

    function setIssuedTokens(data) {
      if (!data || !data.read_token) return;
      issuedTokenEl.value = data.read_token;
      tokenEl.value = data.read_token;
      saveToken();
      setStatus("Token issued. Saved to Access Token field.");
    }

    function fmtBytes(n) {
      const v = Number(n || 0);
      if (!v) return "-";
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = v;
      let idx = 0;
      while (size >= 1024 && idx < units.length - 1) {
        size /= 1024;
        idx += 1;
      }
      const fixed = size >= 100 || idx === 0 ? 0 : 1;
      return size.toFixed(fixed) + " " + units[idx];
    }

    function fmtIso(iso) {
      if (!iso) return "-";
      try {
        return new Date(iso).toISOString().replace("T", " ").replace(".000Z", "Z");
      } catch {
        return iso;
      }
    }

    function setPathLabel(path) {
      if (path === "search results") {
        pathLabelEl.textContent = "Search results";
        return;
      }
      const p = path ? ("/shares/" + path + "/") : "/shares/";
      pathLabelEl.textContent = "Index of " + p;
    }

    function renderBrowseRows(payload) {
      rowsEl.innerHTML = "";
      const items = payload.items || [];
      if (!items.length && !payload.parent) {
        rowsEl.innerHTML = "<tr><td colspan='7'>No files found.</td></tr>";
        return;
      }
      if (payload.parent !== undefined && payload.path) {
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td><a href='#' data-parent='1'>../</a></td>" +
          "<td>-</td><td>-</td><td>-</td><td>dir</td><td>-</td><td>-</td>";
        rowsEl.appendChild(tr);
      }
      for (const item of items) {
        const tr = document.createElement("tr");
        if (item.kind === "dir") {
          tr.innerHTML =
            "<td><a href='#' data-folder='" + esc(item.path) + "'>" + esc(item.name + "/") + "</a></td>" +
            "<td>-</td>" +
            "<td>" + esc(fmtIso(item.updated_at)) + "</td>" +
            "<td>-</td>" +
            "<td>dir</td>" +
            "<td>-</td>" +
            "<td>-</td>";
        } else {
          const suffix = tokenValue() ? ("?token=" + encodeURIComponent(tokenValue())) : "";
          const fileCell = item.downloadable
            ? "<a href='/files/" + encodeURIComponent(item.id) + suffix + "'>" + esc(item.name) + "</a>"
            : esc(item.name);
          tr.innerHTML =
            "<td>" + fileCell + "</td>" +
            "<td>" + esc(fmtBytes(item.file_size)) + "</td>" +
            "<td>" + esc(fmtIso(item.updated_at)) + "</td>" +
            "<td>" + esc(item.source_node || "") + "</td>" +
            "<td>file</td>" +
            "<td>" + esc(item.tags || "") + "</td>" +
            "<td><code>" + esc(item.cid || "") + "</code></td>";
        }
        rowsEl.appendChild(tr);
      }
      rowsEl.querySelectorAll("a[data-folder]").forEach((el) => {
        el.addEventListener("click", (ev) => {
          ev.preventDefault();
          const path = el.getAttribute("data-folder") || "";
          loadBrowse(path);
        });
      });
      rowsEl.querySelectorAll("a[data-parent]").forEach((el) => {
        el.addEventListener("click", (ev) => {
          ev.preventDefault();
          const parent = payload.parent || "";
          loadBrowse(parent);
        });
      });
    }

    function renderSearchRows(entries) {
      rowsEl.innerHTML = "";
      if (!entries.length) {
        rowsEl.innerHTML = "<tr><td colspan='7'>No matches.</td></tr>";
        return;
      }
      for (const e of entries) {
        const file = e.rel_path || e.file_name || e.title || "(unnamed)";
        const suffix = tokenValue() ? ("?token=" + encodeURIComponent(tokenValue())) : "";
        const fileCell = e.downloadable
          ? "<a href='/files/" + encodeURIComponent(e.id) + suffix + "'>" + esc(file) + "</a>"
          : esc(file);
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td>" + fileCell + "</td>" +
          "<td>" + esc(fmtBytes(e.file_size)) + "</td>" +
          "<td>" + esc(fmtIso(e.updated_at)) + "</td>" +
          "<td>" + esc(e.source_node || "") + "</td>" +
          "<td>file</td>" +
          "<td>" + esc(e.tags || "") + "</td>" +
          "<td><code>" + esc(e.cid || "") + "</code></td>";
        rowsEl.appendChild(tr);
      }
    }

    async function loadBrowse(path = "") {
      currentMode = "browse";
      currentBrowsePath = path || "";
      setPathLabel(currentBrowsePath);
      setStatus("Loading directory...");
      try {
        const params = new URLSearchParams();
        if (currentBrowsePath) params.set("path", currentBrowsePath);
        const res = await fetch("/api/browse?" + params.toString(), { headers: authHeaders(false) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Browse failed");
        renderBrowseRows(data);
        setStatus("Showing " + (data.count || 0) + " items in /" + (data.path || "") + ".");
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function searchEntries(query = "") {
      currentMode = "search";
      setPathLabel("search results");
      setStatus("Searching...");
      try {
        const params = new URLSearchParams({ limit: "200" });
        if (query) params.set("query", query);
        const res = await fetch("/api/entries?" + params.toString(), { headers: authHeaders(false) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Load failed");
        renderSearchRows(data.entries || []);
        setStatus("Found " + (data.count || 0) + " matching files.");
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function loadPeers() {
      try {
        const res = await fetch("/api/peers", { headers: authHeaders(false) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Failed loading peers");
        document.getElementById("peerInfo").textContent = "Known peers: " + (data.count || 0);
      } catch {
        document.getElementById("peerInfo").textContent = "Known peers: unavailable";
      }
    }

    async function loadProfile() {
      try {
        const res = await fetch("/api/profile", { headers: authHeaders(false) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Failed loading profile");
        document.getElementById("username").value = data.username || "";
        document.getElementById("sharedDir").value = data.shared_dir || "";
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function saveProfile() {
      const username = document.getElementById("username").value.trim();
      const sharedDir = document.getElementById("sharedDir").value.trim();
      if (!username || !sharedDir) {
        setStatus("Username and shared folder are required.", true);
        return;
      }
      setStatus("Saving profile...");
      try {
        const res = await fetch("/api/profile", {
          method: "POST",
          headers: authHeaders(true),
          body: JSON.stringify({ username: username, shared_dir: sharedDir })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Save profile failed");
        setStatus("Profile saved. Auto-sync is running.");
        await loadBrowse(currentBrowsePath);
        await loadPeers();
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function signupUser() {
      const username = (document.getElementById("authUsername").value || "").trim();
      const password = document.getElementById("authPassword").value || "";
      if (username.length < 3 || password.length < 10) {
        setStatus("Signup needs username (3+) and password (10+).", true);
        return;
      }
      setStatus("Creating account...");
      try {
        const res = await fetch("/api/signup", {
          method: "POST",
          headers: authHeaders(true),
          body: JSON.stringify({ username: username, password: password })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Signup failed");
        setIssuedTokens(data);
        await loadPeers();
        await loadBrowse(currentBrowsePath);
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function loginUser() {
      const username = (document.getElementById("authUsername").value || "").trim();
      const password = document.getElementById("authPassword").value || "";
      if (!username || !password) {
        setStatus("Login needs username and password.", true);
        return;
      }
      setStatus("Logging in...");
      try {
        const res = await fetch("/api/login", {
          method: "POST",
          headers: authHeaders(true),
          body: JSON.stringify({ username: username, password: password })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Login failed");
        setIssuedTokens(data);
        await loadPeers();
        await loadBrowse(currentBrowsePath);
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function rescanFolder() {
      setStatus("Rescanning shared folder...");
      try {
        const res = await fetch("/api/rescan", { method: "POST", headers: authHeaders(false) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Rescan failed");
        setStatus("Scanned " + data.scanned + ", added " + data.added + ", updated " + data.updated + ".");
        await loadBrowse(currentBrowsePath);
      } catch (err) {
        setStatus(String(err), true);
      }
    }

    async function uploadSelected() {
      const files = Array.from(uploadInputEl.files || []);
      if (!files.length) {
        setStatus("Select files or a folder first.", true);
        return;
      }
      let success = 0;
      for (let i = 0; i < files.length; i += 1) {
        const f = files[i];
        const rel = (f.webkitRelativePath && f.webkitRelativePath.trim()) ? f.webkitRelativePath : f.name;
        setStatus("Uploading " + (i + 1) + "/" + files.length + ": " + rel);
        const params = new URLSearchParams({ path: rel });
        try {
          const res = await fetch("/api/upload?" + params.toString(), {
            method: "POST",
            headers: authHeaders(false),
            body: f
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Upload failed");
          success += 1;
        } catch (err) {
          setStatus("Upload failed at " + rel + ": " + String(err), true);
          return;
        }
      }
      setStatus("Uploaded " + success + " file(s).");
      uploadInputEl.value = "";
      await loadBrowse(currentBrowsePath);
    }

    document.getElementById("searchBtn").addEventListener("click", () => {
      const q = document.getElementById("searchQuery").value.trim();
      if (!q) {
        loadBrowse(currentBrowsePath);
        return;
      }
      searchEntries(q);
    });
    document.getElementById("rootBtn").addEventListener("click", () => {
      document.getElementById("searchQuery").value = "";
      loadBrowse("");
    });
    document.getElementById("reloadBtn").addEventListener("click", () => {
      if (currentMode === "search") {
        const q = document.getElementById("searchQuery").value.trim();
        if (q) {
          searchEntries(q);
          return;
        }
      }
      loadBrowse(currentBrowsePath);
    });
    document.getElementById("saveProfileBtn").addEventListener("click", saveProfile);
    document.getElementById("rescanBtn").addEventListener("click", rescanFolder);
    document.getElementById("uploadBtn").addEventListener("click", uploadSelected);
    document.getElementById("signupBtn").addEventListener("click", signupUser);
    document.getElementById("loginBtn").addEventListener("click", loginUser);
    tokenEl.addEventListener("change", () => {
      saveToken();
      loadProfile();
      loadPeers();
      loadBrowse(currentBrowsePath);
    });

    tokenEl.value = localStorage.getItem("peerBayAccessToken") || "";
    loadProfile();
    loadPeers();
    loadBrowse("");
  </script>
</body>
</html>
"""


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_iso_or_none(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except ValueError:
        return None


def ensure_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def normalize_peer_url(url: str) -> str:
    cleaned = url.strip().rstrip("/")
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Peer URL must start with http:// or https://")
    if not parsed.netloc:
        raise ValueError("Peer URL must include host")
    return f"{parsed.scheme}://{parsed.netloc}"


def is_true_env(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def validate_production_config(args: argparse.Namespace, shared_dir: Optional[Path], node_url: Optional[str]) -> None:
    required = [
        ("admin token", args.admin_token),
        ("read token", args.read_token),
        ("mesh token", args.mesh_token),
        ("node url", node_url),
        ("shared dir", str(shared_dir) if shared_dir else None),
    ]
    missing = [name for name, val in required if not val]
    if missing:
        raise ValueError(f"Production mode missing required settings: {', '.join(missing)}")
    if node_url and not node_url.startswith("https://"):
        raise ValueError("Production mode requires https node URL")
    for label, tok in [("admin", args.admin_token), ("read", args.read_token), ("mesh", args.mesh_token)]:
        if tok and len(tok) < 16:
            raise ValueError(f"Production mode requires {label} token length >= 16")


def upsert_peer(conn: sqlite3.Connection, url: str) -> str:
    now = utc_now_iso()
    existing = conn.execute("SELECT url FROM peers WHERE url = ?", (url,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE peers SET last_seen_at = ? WHERE url = ?",
            (now, url),
        )
        conn.commit()
        return "known"
    conn.execute(
        "INSERT INTO peers (url, added_at, last_seen_at, last_sync_at, last_error) VALUES (?, ?, ?, NULL, NULL)",
        (url, now, now),
    )
    conn.commit()
    return "added"


def list_peers(conn: sqlite3.Connection, limit: int = 200) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT url, added_at, last_seen_at, last_sync_at, last_error FROM peers ORDER BY added_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "url": row["url"],
            "added_at": row["added_at"],
            "last_seen_at": row["last_seen_at"],
            "last_sync_at": row["last_sync_at"],
            "last_error": row["last_error"],
        }
        for row in rows
    ]


def get_setting(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if not row:
        return None
    return str(row["value"])


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def issue_api_token(conn: sqlite3.Connection, username: str, role: str) -> str:
    raw = secrets.token_urlsafe(32)
    conn.execute(
        """
        INSERT INTO api_tokens (token_hash, token_prefix, role, username, created_at, revoked_at, last_used_at)
        VALUES (?, ?, ?, ?, ?, NULL, NULL)
        """,
        (hash_token(raw), raw[:8], role, username, utc_now_iso()),
    )
    conn.commit()
    return raw


def revoke_user_tokens(conn: sqlite3.Connection, username: str) -> None:
    conn.execute(
        "UPDATE api_tokens SET revoked_at = ? WHERE username = ? AND revoked_at IS NULL",
        (utc_now_iso(), username),
    )
    conn.commit()


def lookup_api_token(conn: sqlite3.Connection, raw_token: str, role: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT token_hash, role, username FROM api_tokens
        WHERE token_hash = ? AND role = ? AND revoked_at IS NULL
        LIMIT 1
        """,
        (hash_token(raw_token), role),
    ).fetchone()
    if not row:
        return None
    conn.execute(
        "UPDATE api_tokens SET last_used_at = ? WHERE token_hash = ?",
        (utc_now_iso(), row["token_hash"]),
    )
    conn.commit()
    return {"username": row["username"], "role": row["role"]}


def hash_password(password: str, salt: str) -> str:
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200000)
    return derived.hex()


def create_user(conn: sqlite3.Connection, username: str, password: str) -> Dict[str, str]:
    existing = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        raise ValueError("username already exists")
    salt = secrets.token_hex(16)
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO users (username, password_salt, password_hash, created_at, last_login_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (username, salt, hash_password(password, salt), now, now),
    )
    read_token = issue_api_token(conn, username, "read")
    mesh_token = issue_api_token(conn, username, "mesh")
    return {"read_token": read_token, "mesh_token": mesh_token}


def login_user_and_rotate_tokens(conn: sqlite3.Connection, username: str, password: str) -> Dict[str, str]:
    row = conn.execute(
        "SELECT username, password_salt, password_hash FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    if not row:
        raise ValueError("invalid credentials")
    expected = hash_password(password, row["password_salt"])
    if not hmac.compare_digest(expected, row["password_hash"]):
        raise ValueError("invalid credentials")
    revoke_user_tokens(conn, username)
    read_token = issue_api_token(conn, username, "read")
    mesh_token = issue_api_token(conn, username, "mesh")
    conn.execute("UPDATE users SET last_login_at = ? WHERE username = ?", (utc_now_iso(), username))
    conn.commit()
    return {"read_token": read_token, "mesh_token": mesh_token}


def verify_user_credentials(conn: sqlite3.Connection, username: str, password: str) -> bool:
    row = conn.execute(
        "SELECT password_salt, password_hash FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    if not row:
        return False
    expected = hash_password(password, row["password_salt"])
    return hmac.compare_digest(expected, row["password_hash"])


def upsert_p2p_source(
    conn: sqlite3.Connection,
    *,
    cid: str,
    username: str,
    peer_url: str,
    file_name: Optional[str],
    file_size: Optional[int],
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO p2p_sources (cid, username, peer_url, file_name, file_size, announced_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cid, username, peer_url) DO UPDATE SET
          file_name=excluded.file_name,
          file_size=excluded.file_size,
          last_seen_at=excluded.last_seen_at
        """,
        (cid, username, peer_url, file_name, file_size, now, now),
    )
    conn.commit()


def providers_for_cid(conn: sqlite3.Connection, cid: str, limit: int = 50) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT cid, username, peer_url, file_name, file_size, announced_at, last_seen_at
        FROM p2p_sources
        WHERE cid = ?
        ORDER BY last_seen_at DESC
        LIMIT ?
        """,
        (cid, max(1, min(limit, 200))),
    ).fetchall()
    return [
        {
            "cid": row["cid"],
            "username": row["username"],
            "peer_url": row["peer_url"],
            "file_name": row["file_name"],
            "file_size": row["file_size"],
            "announced_at": row["announced_at"],
            "last_seen_at": row["last_seen_at"],
        }
        for row in rows
    ]


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def canonical_signature_payload(entry: Dict[str, Any]) -> bytes:
    payload = {field: entry.get(field) for field in SIGN_FIELDS}
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_entry(entry: Dict[str, Any], secret: Optional[str]) -> Optional[str]:
    if not secret:
        return None
    mac = hmac.new(secret.encode("utf-8"), canonical_signature_payload(entry), hashlib.sha256)
    return mac.hexdigest()


def verify_entry(entry: Dict[str, Any], secret: Optional[str]) -> bool:
    signature = entry.get("signature")
    if not signature:
        return True
    if not secret:
        return False
    expected = sign_entry(entry, secret)
    return hmac.compare_digest(signature, expected or "")


def upsert_entry(conn: sqlite3.Connection, entry: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO entries (
          id, cid, title, description, tags, file_name, file_size, file_mtime,
          local_path, source_node, created_at, updated_at, signature, meta_json
        ) VALUES (
          :id, :cid, :title, :description, :tags, :file_name, :file_size, :file_mtime,
          :local_path, :source_node, :created_at, :updated_at, :signature, :meta_json
        )
        ON CONFLICT(id) DO UPDATE SET
          cid=excluded.cid,
          title=excluded.title,
          description=excluded.description,
          tags=excluded.tags,
          file_name=excluded.file_name,
          file_size=excluded.file_size,
          file_mtime=excluded.file_mtime,
          local_path=excluded.local_path,
          source_node=excluded.source_node,
          created_at=excluded.created_at,
          updated_at=excluded.updated_at,
          signature=excluded.signature,
          meta_json=excluded.meta_json
        """,
        entry,
    )
    conn.commit()


def row_to_entry(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "cid": row["cid"],
        "title": row["title"],
        "description": row["description"],
        "tags": row["tags"],
        "file_name": row["file_name"],
        "file_size": row["file_size"],
        "file_mtime": row["file_mtime"],
        "local_path": row["local_path"],
        "source_node": row["source_node"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "signature": row["signature"],
        "meta_json": row["meta_json"],
    }


def entry_for_client(entry: Dict[str, Any]) -> Dict[str, Any]:
    rel_path = extract_rel_path(entry)
    return {
        "id": entry["id"],
        "cid": entry["cid"],
        "title": entry.get("title"),
        "description": entry.get("description"),
        "tags": entry.get("tags"),
        "file_name": entry.get("file_name"),
        "file_size": entry.get("file_size"),
        "file_mtime": entry.get("file_mtime"),
        "source_node": entry.get("source_node"),
        "created_at": entry.get("created_at"),
        "updated_at": entry.get("updated_at"),
        "downloadable": bool(entry.get("local_path")),
        "rel_path": rel_path,
    }


def entry_for_peer(entry: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(entry)
    out["local_path"] = None
    return out


def extract_rel_path(entry: Dict[str, Any]) -> str:
    meta_raw = entry.get("meta_json")
    if isinstance(meta_raw, str) and meta_raw.strip():
        try:
            meta = json.loads(meta_raw)
            rel = meta.get("rel_path")
            if isinstance(rel, str) and rel.strip():
                return rel.strip().replace("\\", "/")
        except (ValueError, TypeError):
            pass
    file_name = entry.get("file_name")
    if isinstance(file_name, str) and file_name.strip():
        return file_name.strip().replace("\\", "/")
    return ""


def normalize_browse_path(raw: Optional[str]) -> str:
    value = (raw or "").strip().replace("\\", "/")
    value = value.lstrip("/")
    parts: List[str] = []
    for part in value.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)


def browse_entries(conn: sqlite3.Connection, path: str, limit: int = 3000) -> Dict[str, Any]:
    base = normalize_browse_path(path)
    prefix = f"{base}/" if base else ""
    rows = conn.execute(
        "SELECT * FROM entries ORDER BY updated_at DESC LIMIT ?",
        (max(1, min(limit, 10000)),),
    ).fetchall()
    entries = [row_to_entry(r) for r in rows]

    dirs: Dict[str, Dict[str, Any]] = {}
    files: List[Dict[str, Any]] = []

    for entry in entries:
        rel = extract_rel_path(entry)
        if not rel:
            continue
        if prefix and not rel.startswith(prefix):
            continue
        if not prefix and rel.startswith("/"):
            rel = rel.lstrip("/")
        remainder = rel[len(prefix) :] if prefix else rel
        if not remainder:
            continue
        parts = remainder.split("/")
        if len(parts) > 1:
            dirname = parts[0]
            dir_path = f"{prefix}{dirname}" if prefix else dirname
            node = dirs.get(dir_path)
            if not node:
                node = {
                    "kind": "dir",
                    "name": dirname,
                    "path": dir_path,
                    "updated_at": entry.get("updated_at"),
                    "count": 0,
                }
                dirs[dir_path] = node
            node["count"] = int(node["count"]) + 1
            if (entry.get("updated_at") or "") > (node.get("updated_at") or ""):
                node["updated_at"] = entry.get("updated_at")
            continue

        files.append(
            {
                "kind": "file",
                "id": entry.get("id"),
                "name": parts[0],
                "path": rel,
                "file_size": entry.get("file_size"),
                "updated_at": entry.get("updated_at"),
                "source_node": entry.get("source_node"),
                "tags": entry.get("tags"),
                "cid": entry.get("cid"),
                "downloadable": bool(entry.get("local_path")),
            }
        )

    dir_items = sorted(dirs.values(), key=lambda d: d["name"].lower())
    file_items = sorted(files, key=lambda f: f["name"].lower())
    items = dir_items + file_items

    parent = ""
    if base:
        parent = "/".join(base.split("/")[:-1])
    return {"path": base, "parent": parent, "items": items, "count": len(items)}


def add_entry(
    conn: sqlite3.Connection,
    *,
    path: Optional[str],
    cid: Optional[str],
    title: Optional[str],
    description: Optional[str],
    tags: Optional[str],
    source_node: Optional[str],
    secret: Optional[str],
) -> Dict[str, Any]:
    local_path = None
    file_name = None
    file_size = None
    file_mtime = None

    if path:
        p = Path(path).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"Not a file: {p}")
        local_path = str(p)
        file_name = p.name
        stat = p.stat()
        file_size = stat.st_size
        file_mtime = stat.st_mtime
        cid = cid or file_sha256(p)
    elif not cid:
        raise ValueError("Either --path or --cid is required")

    now = utc_now_iso()
    entry = {
        "id": str(uuid.uuid4()),
        "cid": cid,
        "title": title,
        "description": description,
        "tags": tags,
        "file_name": file_name,
        "file_size": file_size,
        "file_mtime": file_mtime,
        "local_path": local_path,
        "source_node": source_node,
        "created_at": now,
        "updated_at": now,
        "signature": None,
        "meta_json": None,
    }
    entry["signature"] = sign_entry(entry, secret)
    upsert_entry(conn, entry)
    return entry


def list_entries(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM entries ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [row_to_entry(r) for r in rows]


def search_entries(conn: sqlite3.Connection, query: str, limit: int = 100) -> List[Dict[str, Any]]:
    like = f"%{query}%"
    rows = conn.execute(
        """
        SELECT * FROM entries
        WHERE cid LIKE ? OR IFNULL(title, '') LIKE ? OR IFNULL(description, '') LIKE ?
          OR IFNULL(tags, '') LIKE ? OR IFNULL(file_name, '') LIKE ?
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (like, like, like, like, like, limit),
    ).fetchall()
    return [row_to_entry(r) for r in rows]


def export_entries(conn: sqlite3.Connection, since: Optional[str], limit: int) -> List[Dict[str, Any]]:
    params: List[Any] = []
    sql = "SELECT * FROM entries"
    if since:
        sql += " WHERE updated_at > ?"
        params.append(since)
    sql += " ORDER BY updated_at ASC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [entry_for_peer(row_to_entry(r)) for r in rows]


def refresh_path_entry(
    conn: sqlite3.Connection,
    *,
    path: str,
    tags: Optional[str],
    source_node: Optional[str],
    secret: Optional[str],
    shared_dir: Optional[Path] = None,
) -> str:
    p = Path(path).expanduser().resolve()
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Not a file: {p}")

    cid = file_sha256(p)
    stat = p.stat()
    now = utc_now_iso()
    rel_path = p.name
    if shared_dir is not None:
        try:
            rel_path = str(p.relative_to(shared_dir)).replace("\\", "/")
        except ValueError:
            rel_path = p.name

    existing = conn.execute("SELECT * FROM entries WHERE cid = ? LIMIT 1", (cid,)).fetchone()
    if existing:
        entry = row_to_entry(existing)
        entry["title"] = entry.get("title") or p.stem
        entry["file_name"] = p.name
        entry["file_size"] = stat.st_size
        entry["file_mtime"] = stat.st_mtime
        entry["local_path"] = str(p)
        entry["tags"] = tags if tags is not None else entry.get("tags")
        entry["source_node"] = source_node if source_node is not None else entry.get("source_node")
        entry["updated_at"] = now
        entry["meta_json"] = json.dumps({"rel_path": rel_path}, separators=(",", ":"))
        entry["signature"] = None
        entry["signature"] = sign_entry(entry, secret)
        upsert_entry(conn, entry)
        return "updated"

    entry = {
        "id": str(uuid.uuid4()),
        "cid": cid,
        "title": p.stem,
        "description": None,
        "tags": tags,
        "file_name": p.name,
        "file_size": stat.st_size,
        "file_mtime": stat.st_mtime,
        "local_path": str(p),
        "source_node": source_node,
        "created_at": now,
        "updated_at": now,
        "signature": None,
        "meta_json": json.dumps({"rel_path": rel_path}, separators=(",", ":")),
    }
    entry["signature"] = sign_entry(entry, secret)
    upsert_entry(conn, entry)
    return "added"


def pull_from_peer(
    conn: sqlite3.Connection,
    *,
    peer: str,
    since: Optional[str],
    limit: int,
    timeout: int,
    secret: Optional[str],
    mesh_token: Optional[str] = None,
) -> Dict[str, Any]:
    url = f"{peer.rstrip('/')}/entries?limit={limit}"
    if since:
        url += f"&since={since}"

    headers = {"Accept": "application/json"}
    if mesh_token:
        headers["X-Mesh-Token"] = mesh_token
    req = Request(url, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    entries = payload.get("entries", [])
    imported = 0
    skipped = 0

    for entry in entries:
        if not verify_entry(entry, secret):
            skipped += 1
            continue
        normalized = {
            "id": entry["id"],
            "cid": entry["cid"],
            "title": entry.get("title"),
            "description": entry.get("description"),
            "tags": entry.get("tags"),
            "file_name": entry.get("file_name"),
            "file_size": entry.get("file_size"),
            "file_mtime": entry.get("file_mtime"),
            "local_path": entry.get("local_path"),
            "source_node": entry.get("source_node"),
            "created_at": entry["created_at"],
            "updated_at": entry["updated_at"],
            "signature": entry.get("signature"),
            "meta_json": entry.get("meta_json"),
        }
        upsert_entry(conn, normalized)
        imported += 1

    return {"imported": imported, "skipped": skipped, "peer": peer, "received": len(entries)}


def announce_to_peer(peer: str, node_url: Optional[str], timeout: int, mesh_token: Optional[str]) -> List[str]:
    if not node_url:
        return []
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if mesh_token:
        headers["X-Mesh-Token"] = mesh_token
    req = Request(
        f"{peer.rstrip('/')}/api/announce",
        data=json.dumps({"peer": node_url}).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    peers = payload.get("peers", [])
    out: List[str] = []
    for item in peers:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict) and item.get("url"):
            out.append(item["url"])
    return out


def sync_known_peers(
    *,
    db_path: Path,
    secret: Optional[str],
    node_url: Optional[str],
    timeout: int,
    limit: int,
    mesh_token: Optional[str],
) -> Dict[str, Any]:
    conn = ensure_db(db_path)
    try:
        peers = [row["url"] for row in conn.execute("SELECT url FROM peers").fetchall()]
    finally:
        conn.close()

    total_imported = 0
    total_skipped = 0
    total_errors = 0
    synced = 0

    for peer in peers:
        try:
            peer_norm = normalize_peer_url(peer)
        except ValueError:
            continue
        if node_url and peer_norm == node_url:
            continue

        conn = ensure_db(db_path)
        try:
            # Mutual announce allows peer graph discovery from a small bootstrap set.
            try:
                discovered = announce_to_peer(peer_norm, node_url, timeout, mesh_token)
            except Exception:
                discovered = []
            for d in discovered:
                try:
                    d_norm = normalize_peer_url(d)
                except ValueError:
                    continue
                if node_url and d_norm == node_url:
                    continue
                upsert_peer(conn, d_norm)

            result = pull_from_peer(
                conn,
                peer=peer_norm,
                since=None,
                limit=limit,
                timeout=timeout,
                secret=secret,
                mesh_token=mesh_token,
            )
            now = utc_now_iso()
            conn.execute(
                "UPDATE peers SET last_sync_at = ?, last_seen_at = ?, last_error = NULL WHERE url = ?",
                (now, now, peer_norm),
            )
            conn.commit()
            total_imported += int(result.get("imported", 0))
            total_skipped += int(result.get("skipped", 0))
            synced += 1
        except Exception as exc:
            total_errors += 1
            conn.execute(
                "UPDATE peers SET last_error = ?, last_seen_at = ? WHERE url = ?",
                (str(exc), utc_now_iso(), peer_norm),
            )
            conn.commit()
        finally:
            conn.close()

    return {
        "synced_peers": synced,
        "imported": total_imported,
        "skipped": total_skipped,
        "errors": total_errors,
    }


def cmd_add(args: argparse.Namespace) -> None:
    conn = ensure_db(Path(args.db))
    try:
        entry = add_entry(
            conn,
            path=args.path,
            cid=args.cid,
            title=args.title,
            description=args.description,
            tags=args.tags,
            source_node=args.source,
            secret=args.secret,
        )
    finally:
        conn.close()
    print(json.dumps(entry, indent=2))


def cmd_bulk_add(args: argparse.Namespace) -> None:
    conn = ensure_db(Path(args.db))
    files = [p for p in Path(args.dir).expanduser().resolve().rglob("*") if p.is_file()]
    added = 0
    try:
        for file_path in files:
            add_entry(
                conn,
                path=str(file_path),
                cid=None,
                title=None,
                description=None,
                tags=args.tags,
                source_node=args.source,
                secret=args.secret,
            )
            added += 1
    finally:
        conn.close()
    print(json.dumps({"added": added, "directory": str(Path(args.dir).resolve())}, indent=2))


def cmd_list(args: argparse.Namespace) -> None:
    conn = ensure_db(Path(args.db))
    try:
        out = list_entries(conn, args.limit)
    finally:
        conn.close()
    print(json.dumps(out, indent=2))


def cmd_search(args: argparse.Namespace) -> None:
    conn = ensure_db(Path(args.db))
    try:
        out = search_entries(conn, args.query, args.limit)
    finally:
        conn.close()
    print(json.dumps(out, indent=2))


def cmd_pull(args: argparse.Namespace) -> None:
    conn = ensure_db(Path(args.db))
    since = None
    if args.since:
        parsed = parse_iso_or_none(args.since)
        if not parsed:
            raise ValueError("Invalid --since datetime; use ISO8601")
        since = parsed.isoformat()

    try:
        result = pull_from_peer(
            conn,
            peer=args.peer,
            since=since,
            limit=args.limit,
            timeout=args.timeout,
            secret=args.secret,
            mesh_token=args.mesh_token,
        )
    finally:
        conn.close()

    print(json.dumps(result, indent=2))


class ArchiveHandler(BaseHTTPRequestHandler):
    def __init__(
        self,
        *args,
        db_path: Path,
        secret: Optional[str],
        state: Dict[str, Any],
        **kwargs,
    ):
        self.db_path = db_path
        self.secret = secret
        self.state = state
        super().__init__(*args, **kwargs)

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store")
        if self.state.get("production"):
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, content: str, status: int = 200) -> None:
        body = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        )
        if self.state.get("production"):
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        if length > MAX_REQUEST_BODY:
            raise ValueError("request body too large")
        raw = self.rfile.read(length).decode("utf-8")
        if not raw.strip():
            return {}
        return json.loads(raw)

    def _token_from_request(self) -> Optional[str]:
        auth = self.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        api_key = self.headers.get("X-API-Key", "").strip()
        if api_key:
            return api_key
        parsed = urlparse(self.path)
        token = parse_qs(parsed.query).get("token", [None])[0]
        return token.strip() if isinstance(token, str) else token

    def _basic_credentials(self) -> Optional[tuple[str, str]]:
        auth = self.headers.get("Authorization", "")
        if not auth.lower().startswith("basic "):
            return None
        raw = auth[6:].strip()
        if not raw:
            return None
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
        except (ValueError, binascii.Error, UnicodeDecodeError):
            return None
        if ":" not in decoded:
            return None
        username, password = decoded.split(":", 1)
        username = username.strip()
        if not username:
            return None
        return username, password

    def _username_from_basic_auth(self) -> Optional[str]:
        creds = self._basic_credentials()
        if not creds:
            return None
        username, password = creds
        conn = ensure_db(self.db_path)
        try:
            ok = verify_user_credentials(conn, username, password)
        finally:
            conn.close()
        if not ok:
            return None
        return username

    def _require_admin(self) -> bool:
        admin_token = self.state.get("admin_token")
        if not admin_token:
            return True
        if self._token_from_request() == admin_token:
            return True
        self._send_json({"error": "unauthorized"}, status=401)
        return False

    def _require_read(self) -> bool:
        read_token = self.state.get("read_token")
        token = self._token_from_request()
        admin_token = self.state.get("admin_token")
        if token and token in {read_token, admin_token}:
            return True
        if token:
            conn = ensure_db(self.db_path)
            try:
                hit = lookup_api_token(conn, token, "read")
            finally:
                conn.close()
            if hit:
                return True
        if self._username_from_basic_auth():
            return True
        if not read_token:
            return True
        self._send_json({"error": "unauthorized"}, status=401)
        return False

    def _require_mesh(self) -> bool:
        mesh_token = self.state.get("mesh_token")
        token = self.headers.get("X-Mesh-Token", "").strip()
        if token and token == mesh_token:
            return True
        if token:
            conn = ensure_db(self.db_path)
            try:
                hit = lookup_api_token(conn, token, "mesh")
            finally:
                conn.close()
            if hit:
                return True
        if self._username_from_basic_auth():
            return True
        if not mesh_token:
            return True
        self._send_json({"error": "mesh unauthorized"}, status=401)
        return False

    def _resolve_upload_username(self) -> Optional[str]:
        username = self._username_from_basic_auth()
        if username:
            return username
        token = self._token_from_request()
        if not token:
            return None
        if token == self.state.get("admin_token"):
            return str(self.state.get("username") or "admin")
        conn = ensure_db(self.db_path)
        try:
            hit = lookup_api_token(conn, token, "read")
        finally:
            conn.close()
        if not hit:
            return None
        return str(hit.get("username") or "").strip() or None

    def _rate_limited(self, key: str, limit: int, window_seconds: int) -> bool:
        ip = self.client_address[0] if self.client_address else "unknown"
        bucket_key = f"{ip}:{key}"
        now = time.time()
        buckets = self.state.setdefault("rate_limits", {})
        entries = buckets.get(bucket_key, [])
        cutoff = now - window_seconds
        entries = [ts for ts in entries if ts >= cutoff]
        if len(entries) >= limit:
            self._send_json({"error": "rate limited"}, status=429)
            buckets[bucket_key] = entries
            return True
        entries.append(now)
        buckets[bucket_key] = entries
        return False

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(INDEX_HTML)
            return

        if parsed.path == "/health":
            self._send_json({"ok": True, "service": "p2p-archive-index", "node_url": self.state.get("node_url")})
            return

        query = parse_qs(parsed.query)
        if parsed.path == "/api/profile":
            if not self._require_admin():
                return
            self._send_json(
                {
                    "username": self.state.get("username"),
                    "shared_dir": str(self.state.get("shared_dir")) if self.state.get("shared_dir") else None,
                    "node_url": self.state.get("node_url"),
                    "production": bool(self.state.get("production")),
                    "allow_signup": bool(self.state.get("allow_signup")),
                }
            )
            return

        if parsed.path == "/api/peers":
            if not self._require_admin():
                return
            conn = ensure_db(self.db_path)
            try:
                peers = list_peers(conn, 500)
            finally:
                conn.close()
            self._send_json({"peers": peers, "count": len(peers), "node_url": self.state.get("node_url")})
            return

        if parsed.path == "/api/entries":
            if not self._require_read():
                return
            limit = int(query.get("limit", ["200"])[0])
            limit = max(1, min(limit, 1000))
            search_q = query.get("query", [""])[0].strip()

            conn = ensure_db(self.db_path)
            try:
                if search_q:
                    entries = search_entries(conn, search_q, limit)
                else:
                    entries = list_entries(conn, limit)
            finally:
                conn.close()
            self._send_json({"entries": [entry_for_client(e) for e in entries], "count": len(entries)})
            return

        if parsed.path == "/api/browse":
            if not self._require_read():
                return
            browse_path = query.get("path", [""])[0]
            conn = ensure_db(self.db_path)
            try:
                payload = browse_entries(conn, browse_path, limit=5000)
            finally:
                conn.close()
            self._send_json(payload)
            return

        if parsed.path == "/api/p2p/providers":
            if not self._require_read():
                return
            cid = (query.get("cid", [""])[0] or "").strip().lower()
            if not cid:
                self._send_json({"error": "cid is required"}, status=400)
                return
            conn = ensure_db(self.db_path)
            try:
                providers = providers_for_cid(conn, cid, limit=50)
            finally:
                conn.close()
            self._send_json({"cid": cid, "providers": providers, "count": len(providers)})
            return

        if parsed.path != "/entries":
            if parsed.path.startswith("/files/"):
                if not self._require_read():
                    return
                if self._rate_limited("files", 240, 60):
                    return
                entry_id = parsed.path.split("/", 2)[2]
                conn = ensure_db(self.db_path)
                try:
                    row = conn.execute("SELECT * FROM entries WHERE id = ?", (entry_id,)).fetchone()
                finally:
                    conn.close()
                if not row:
                    self._send_json({"error": "not found"}, status=404)
                    return
                entry = row_to_entry(row)
                local_path = entry.get("local_path")
                if not local_path:
                    self._send_json({"error": "file unavailable"}, status=404)
                    return
                file_path = Path(local_path).resolve()
                if not file_path.exists() or not file_path.is_file():
                    self._send_json({"error": "file unavailable"}, status=404)
                    return
                shared_dir = self.state.get("shared_dir")
                if shared_dir is not None:
                    try:
                        file_path.relative_to(shared_dir)
                    except ValueError:
                        self._send_json({"error": "forbidden"}, status=403)
                        return
                ctype = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(file_path.stat().st_size))
                self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{quote(file_path.name)}")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Cache-Control", "private, max-age=0")
                self.end_headers()
                with file_path.open("rb") as fh:
                    while True:
                        chunk = fh.read(64 * 1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                return
            self._send_json({"error": "not found"}, status=404)
            return

        if not self._require_mesh():
            return

        limit = int(query.get("limit", ["500"])[0])
        limit = max(1, min(limit, 5000))
        since = query.get("since", [None])[0]

        conn = ensure_db(self.db_path)
        try:
            entries = export_entries(conn, since, limit)
        finally:
            conn.close()

        self._send_json({"entries": entries, "count": len(entries)})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/p2p/publish":
            if self._rate_limited("p2p_publish", 180, 60):
                return
            username = self._resolve_upload_username()
            if not username:
                self._send_json({"error": "unauthorized"}, status=401)
                return
            try:
                payload = self._read_json()
                peer_url = normalize_peer_url(str(payload.get("peer_url", "")).strip())
                cid = str(payload.get("cid", "")).strip().lower()
                if len(cid) < 16:
                    raise ValueError("invalid cid")
                file_name = payload.get("file_name")
                file_size = payload.get("file_size")
                if file_size is not None:
                    file_size = int(file_size)
                    if file_size < 0:
                        raise ValueError("invalid file_size")

                conn = ensure_db(self.db_path)
                try:
                    upsert_p2p_source(
                        conn,
                        cid=cid,
                        username=username,
                        peer_url=peer_url,
                        file_name=file_name,
                        file_size=file_size,
                    )
                finally:
                    conn.close()
                self._send_json({"ok": True, "cid": cid, "peer_url": peer_url, "username": username})
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/upload":
            if self._rate_limited("upload", 120, 60):
                return
            shared_dir = self.state.get("shared_dir")
            if shared_dir is None:
                self._send_json({"error": "shared folder is not configured"}, status=400)
                return
            uploader = self._resolve_upload_username()
            if not uploader:
                self._send_json({"error": "unauthorized"}, status=401)
                return
            try:
                query = parse_qs(parsed.query)
                rel_raw = query.get("path", [""])[0]
                rel_path = normalize_browse_path(rel_raw)
                if not rel_path:
                    raise ValueError("path is required")
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0:
                    raise ValueError("request body is empty")
                max_bytes = int(self.state.get("max_upload_bytes", 50 * 1024 * 1024))
                if length > max_bytes:
                    self._send_json({"error": "file too large"}, status=413)
                    return

                base = Path(shared_dir).resolve()
                user_root = (base / "users" / uploader).resolve()
                target = (user_root / rel_path).resolve()
                try:
                    target.relative_to(user_root)
                except ValueError:
                    raise ValueError("invalid upload path")
                target.parent.mkdir(parents=True, exist_ok=True)

                remaining = length
                with target.open("wb") as fh:
                    while remaining > 0:
                        chunk = self.rfile.read(min(64 * 1024, remaining))
                        if not chunk:
                            break
                        fh.write(chunk)
                        remaining -= len(chunk)
                if remaining != 0:
                    raise ValueError("incomplete upload body")

                conn = ensure_db(self.db_path)
                try:
                    refresh_path_entry(
                        conn,
                        path=str(target),
                        tags=None,
                        source_node=uploader,
                        secret=self.secret,
                        shared_dir=base,
                    )
                finally:
                    conn.close()
                self._send_json(
                    {
                        "ok": True,
                        "uploader": uploader,
                        "path": f"users/{uploader}/{rel_path}",
                        "bytes": length,
                    },
                    status=201,
                )
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/signup":
            if not self.state.get("allow_signup"):
                self._send_json({"error": "signup disabled"}, status=403)
                return
            if self._rate_limited("signup", 15, 60):
                return
            try:
                payload = self._read_json()
                username = str(payload.get("username", "")).strip()
                password = str(payload.get("password", ""))
                if len(username) < 3:
                    raise ValueError("username must be at least 3 chars")
                if len(password) < 10:
                    raise ValueError("password must be at least 10 chars")
                conn = ensure_db(self.db_path)
                try:
                    create_user(conn, username, password)
                finally:
                    conn.close()
                self._send_json(
                    {
                        "ok": True,
                        "username": username,
                    },
                    status=201,
                )
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/login":
            if self._rate_limited("login", 30, 60):
                return
            try:
                payload = self._read_json()
                username = str(payload.get("username", "")).strip()
                password = str(payload.get("password", ""))
                if not username or not password:
                    raise ValueError("username and password are required")
                conn = ensure_db(self.db_path)
                try:
                    row = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
                    if not row:
                        raise ValueError("invalid credentials")
                    if not verify_user_credentials(conn, username, password):
                        raise ValueError("invalid credentials")
                    conn.execute("UPDATE users SET last_login_at = ? WHERE username = ?", (utc_now_iso(), username))
                    conn.commit()
                finally:
                    conn.close()
                self._send_json(
                    {
                        "ok": True,
                        "username": username,
                    }
                )
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/add":
            if not self._require_admin():
                return
            if self._rate_limited("add", 30, 60):
                return
            try:
                payload = self._read_json()
                # Public web mode should not allow arbitrary host-path indexing by default.
                path_value = payload.get("path")
                if path_value and not env_flag("ALLOW_WEB_PATH_ADD", default=False):
                    raise ValueError("path-based add is disabled for web requests")

                conn = ensure_db(self.db_path)
                try:
                    entry = add_entry(
                        conn,
                        path=path_value,
                        cid=payload.get("cid"),
                        title=payload.get("title"),
                        description=payload.get("description"),
                        tags=payload.get("tags"),
                        source_node=payload.get("source"),
                        secret=self.secret,
                    )
                finally:
                    conn.close()
                self._send_json({"entry": entry}, status=201)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/pull":
            if not self._require_admin():
                return
            if self._rate_limited("pull", 20, 60):
                return
            try:
                payload = self._read_json()
                peer = payload.get("peer")
                if not peer:
                    raise ValueError("peer is required")

                since_raw = payload.get("since")
                since = None
                if since_raw:
                    parsed_since = parse_iso_or_none(since_raw)
                    if not parsed_since:
                        raise ValueError("Invalid since datetime; use ISO8601")
                    since = parsed_since.isoformat()

                conn = ensure_db(self.db_path)
                try:
                    result = pull_from_peer(
                        conn,
                        peer=peer,
                        since=since,
                        limit=max(1, min(int(payload.get("limit", 500)), 5000)),
                        timeout=max(1, min(int(payload.get("timeout", 20)), 120)),
                        secret=self.secret,
                        mesh_token=self.state.get("mesh_token"),
                    )
                finally:
                    conn.close()
                self._send_json(result)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/peers":
            if not self._require_admin():
                return
            if self._rate_limited("peers", 60, 60):
                return
            try:
                payload = self._read_json()
                peer = payload.get("peer")
                if not peer:
                    raise ValueError("peer is required")
                peer_norm = normalize_peer_url(peer)
                node_url = self.state.get("node_url")
                if node_url and peer_norm == node_url:
                    raise ValueError("peer cannot be this node")
                conn = ensure_db(self.db_path)
                try:
                    status = upsert_peer(conn, peer_norm)
                finally:
                    conn.close()
                self._send_json({"status": status, "peer": peer_norm}, status=201 if status == "added" else 200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/announce":
            if not self._require_mesh():
                return
            if self._rate_limited("announce", 120, 60):
                return
            try:
                payload = self._read_json()
                peer = payload.get("peer")
                conn = ensure_db(self.db_path)
                try:
                    if peer:
                        peer_norm = normalize_peer_url(peer)
                        node_url = self.state.get("node_url")
                        if not node_url or peer_norm != node_url:
                            upsert_peer(conn, peer_norm)
                    peers = list_peers(conn, 500)
                finally:
                    conn.close()
                self._send_json({"ok": True, "node_url": self.state.get("node_url"), "peers": peers})
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/profile":
            if not self._require_admin():
                return
            if self._rate_limited("profile", 30, 60):
                return
            try:
                payload = self._read_json()
                username = str(payload.get("username", "")).strip()
                shared_raw = str(payload.get("shared_dir", "")).strip()
                if not username:
                    raise ValueError("username is required")
                shared_dir = self.state.get("shared_dir")
                if self.state.get("production"):
                    if shared_raw:
                        desired = str(Path(shared_raw).expanduser().resolve())
                        current = str(shared_dir) if shared_dir else ""
                        if current and desired != current:
                            raise ValueError("shared_dir is locked in production mode")
                    if shared_dir is None:
                        raise ValueError("shared_dir must be configured at startup in production mode")
                else:
                    if not shared_raw:
                        raise ValueError("shared_dir is required")
                    shared_dir = Path(shared_raw).expanduser().resolve()
                    shared_dir.mkdir(parents=True, exist_ok=True)

                conn = ensure_db(self.db_path)
                try:
                    set_setting(conn, "username", username)
                    if shared_dir:
                        set_setting(conn, "shared_dir", str(shared_dir))
                finally:
                    conn.close()

                self.state["username"] = username
                if shared_dir:
                    self.state["shared_dir"] = shared_dir
                self._send_json({"ok": True, "username": username, "shared_dir": str(self.state.get("shared_dir"))})
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if parsed.path == "/api/rescan":
            if not self._require_admin():
                return
            if self._rate_limited("rescan", 20, 60):
                return
            shared_dir = self.state.get("shared_dir")
            if shared_dir is None:
                self._send_json({"error": "shared folder is not configured"}, status=400)
                return
            try:
                files = [p for p in shared_dir.rglob("*") if p.is_file()]
                scanned = len(files)
                added = 0
                updated = 0
                source_name = self.state.get("username") or "shared-folder"
                conn = ensure_db(self.db_path)
                try:
                    for p in files:
                        action = refresh_path_entry(
                            conn,
                            path=str(p),
                            tags=None,
                            source_node=source_name,
                            secret=self.secret,
                            shared_dir=shared_dir,
                        )
                        if action == "added":
                            added += 1
                        else:
                            updated += 1
                finally:
                    conn.close()
                self._send_json(
                    {
                        "shared_dir": str(shared_dir),
                        "scanned": scanned,
                        "added": added,
                        "updated": updated,
                    }
                )
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def make_handler(
    db_path: Path,
    secret: Optional[str],
    state: Dict[str, Any],
):
    def handler(*args, **kwargs):
        return ArchiveHandler(
            *args,
            db_path=db_path,
            secret=secret,
            state=state,
            **kwargs,
        )

    return handler


def cmd_serve(args: argparse.Namespace) -> None:
    db_path = Path(args.db)
    conn = ensure_db(db_path)
    try:
        saved_username = get_setting(conn, "username")
        saved_shared_dir = get_setting(conn, "shared_dir")
    finally:
        conn.close()

    shared_dir = None
    shared_dir_raw = args.shared_dir or saved_shared_dir
    if shared_dir_raw:
        shared_dir = Path(shared_dir_raw).expanduser().resolve()
        shared_dir.mkdir(parents=True, exist_ok=True)
    username = args.username or saved_username
    node_url = normalize_peer_url(args.node_url) if args.node_url else None
    if args.production:
        validate_production_config(args, shared_dir, node_url)
    state: Dict[str, Any] = {
        "username": username,
        "shared_dir": shared_dir,
        "node_url": node_url,
        "admin_token": args.admin_token,
        "read_token": args.read_token,
        "mesh_token": args.mesh_token,
        "production": bool(args.production),
        "allow_signup": bool(args.allow_signup),
        "max_upload_bytes": int(max(1, args.max_upload_mb) * 1024 * 1024),
        "rate_limits": {},
    }

    conn = ensure_db(db_path)
    try:
        if username:
            set_setting(conn, "username", username)
        if shared_dir:
            set_setting(conn, "shared_dir", str(shared_dir))
        if args.bootstrap_peers:
            for peer in [p.strip() for p in args.bootstrap_peers.split(",") if p.strip()]:
                try:
                    peer_norm = normalize_peer_url(peer)
                except ValueError:
                    continue
                if node_url and peer_norm == node_url:
                    continue
                upsert_peer(conn, peer_norm)
    finally:
        conn.close()

    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(db_path, args.secret, state))
    print(
        json.dumps(
            {
                "status": "serving",
                "host": args.host,
                "port": args.port,
                "db": str(db_path.resolve()),
                "shared_dir": str(shared_dir) if shared_dir else None,
                "node_url": node_url,
                "username": username,
                "auto_sync_seconds": args.auto_sync_interval,
                "auth": {
                    "admin_token": bool(args.admin_token),
                    "read_token": bool(args.read_token),
                    "mesh_token": bool(args.mesh_token),
                },
                "production": bool(args.production),
                "allow_signup": bool(args.allow_signup),
                "max_upload_mb": int(max(1, args.max_upload_mb)),
            },
            indent=2,
        )
    )

    stop_event = threading.Event()

    def wait_for_stop() -> None:
        try:
            while not stop_event.is_set():
                stop_event.wait(0.2)
        except KeyboardInterrupt:
            pass

    watcher = threading.Thread(target=wait_for_stop, daemon=True)
    watcher.start()

    def auto_sync_loop() -> None:
        while not stop_event.is_set():
            try:
                sync_known_peers(
                    db_path=db_path,
                    secret=args.secret,
                    node_url=node_url,
                    timeout=args.sync_timeout,
                    limit=args.sync_limit,
                    mesh_token=args.mesh_token,
                )
            except Exception:
                pass
            stop_event.wait(max(1, args.auto_sync_interval))

    if args.auto_sync_interval > 0:
        sync_thread = threading.Thread(target=auto_sync_loop, daemon=True)
        sync_thread.start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        httpd.shutdown()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Peer-to-peer file archive index CLI")
    parser.add_argument(
        "--db",
        default=os.getenv("ARCHIVE_INDEX_DB", "archive_index.db"),
        help="Path to SQLite DB",
    )
    parser.add_argument("--secret", default=os.getenv("ARCHIVE_INDEX_SECRET"), help="HMAC secret")

    sub = parser.add_subparsers(dest="command", required=True)

    add = sub.add_parser("add", help="Add one archive entry")
    add.add_argument("--path", help="Local file path")
    add.add_argument("--cid", help="Content hash/id if file is not local")
    add.add_argument("--title")
    add.add_argument("--description")
    add.add_argument("--tags", help="Comma-separated tags")
    add.add_argument("--source", help="Source node identifier")
    add.set_defaults(func=cmd_add)

    bulk = sub.add_parser("bulk-add", help="Index all files under a directory")
    bulk.add_argument("--dir", required=True)
    bulk.add_argument("--tags", help="Tags applied to all indexed files")
    bulk.add_argument("--source", help="Source node identifier")
    bulk.set_defaults(func=cmd_bulk_add)

    ls_cmd = sub.add_parser("list", help="List recent entries")
    ls_cmd.add_argument("--limit", type=int, default=100)
    ls_cmd.set_defaults(func=cmd_list)

    search = sub.add_parser("search", help="Search entries")
    search.add_argument("query")
    search.add_argument("--limit", type=int, default=100)
    search.set_defaults(func=cmd_search)

    serve = sub.add_parser("serve", help="Serve web UI + peer API over HTTP")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument(
        "--shared-dir",
        default=os.getenv("ARCHIVE_SHARED_DIR"),
        help="Folder to expose and rescan from website",
    )
    serve.add_argument(
        "--username",
        default=os.getenv("ARCHIVE_USERNAME"),
        help="Display name used as source for this node's shared files",
    )
    serve.add_argument(
        "--node-url",
        default=os.getenv("ARCHIVE_NODE_URL"),
        help="Public URL for this node (used for peer announce)",
    )
    serve.add_argument(
        "--bootstrap-peers",
        default=os.getenv("ARCHIVE_BOOTSTRAP_PEERS", ""),
        help="Comma-separated peer URLs to seed peer discovery",
    )
    serve.add_argument(
        "--auto-sync-interval",
        type=int,
        default=int(os.getenv("ARCHIVE_AUTO_SYNC_INTERVAL", "30")),
        help="Auto-sync interval in seconds",
    )
    serve.add_argument(
        "--sync-timeout",
        type=int,
        default=int(os.getenv("ARCHIVE_SYNC_TIMEOUT", "15")),
        help="Timeout seconds for peer sync requests",
    )
    serve.add_argument(
        "--sync-limit",
        type=int,
        default=int(os.getenv("ARCHIVE_SYNC_LIMIT", "800")),
        help="Max entries pulled from each peer per sync cycle",
    )
    serve.add_argument(
        "--admin-token",
        default=os.getenv("ARCHIVE_ADMIN_TOKEN"),
        help="Bearer token required for admin actions",
    )
    serve.add_argument(
        "--read-token",
        default=os.getenv("ARCHIVE_READ_TOKEN"),
        help="Bearer token required for listing/downloads",
    )
    serve.add_argument(
        "--mesh-token",
        default=os.getenv("ARCHIVE_MESH_TOKEN"),
        help="Token used for peer mesh endpoints (/entries, /api/announce)",
    )
    serve.add_argument(
        "--production",
        action="store_true",
        default=is_true_env(os.getenv("ARCHIVE_PRODUCTION")),
        help="Enable strict production safety checks",
    )
    serve.add_argument(
        "--allow-signup",
        action=argparse.BooleanOptionalAction,
        default=True if os.getenv("ARCHIVE_ALLOW_SIGNUP") is None else is_true_env(os.getenv("ARCHIVE_ALLOW_SIGNUP")),
        help="Allow public self-signup to generate per-user tokens",
    )
    serve.add_argument(
        "--max-upload-mb",
        type=int,
        default=int(os.getenv("ARCHIVE_MAX_UPLOAD_MB", "200")),
        help="Maximum upload size per file in MB",
    )
    serve.set_defaults(func=cmd_serve)

    pull = sub.add_parser("pull", help="Pull entries from another peer")
    pull.add_argument("--peer", required=True, help="Peer base URL, e.g. http://127.0.0.1:8787")
    pull.add_argument("--since", help="Only import entries updated after ISO8601 timestamp")
    pull.add_argument("--limit", type=int, default=500)
    pull.add_argument("--timeout", type=int, default=20)
    pull.add_argument("--mesh-token", default=os.getenv("ARCHIVE_MESH_TOKEN"))
    pull.set_defaults(func=cmd_pull)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
