#!/usr/bin/env python3
"""
MQTT-Monitor-Dashboard-MK1
===========================
Einzeldatei-Flask-Anwendung nach dem Design-Vorbild von "BambuLab-MQTT-Dashboard-MK6".

Stellt einen eingebetteten MQTT-Broker (amqtt) im lokalen Netzwerk zur Verfuegung
und visualisiert alle eintreffenden Pakete/Topics live als Knotengraph
("MQTT-Diagramm"): Broker in der Mitte, Clients und Topics als Knoten drumherum,
Verbindungslinien pulsieren bei jedem Paket. Darunter ein chronologisches
Paket-Log sowie Client-/Topic-Tabellen.

Konfiguration (Host/Port des Brokers, Web-Port, Zugangsdaten) liegt bewusst in
config.json und nicht im Quellcode, damit sie ohne Codeaenderung angepasst
werden kann.
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass

from flask import Flask, Response, jsonify, request

from amqtt.broker import Broker
from amqtt.plugins.authentication import BaseAuthPlugin
from amqtt.plugins.base import BasePlugin

VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Pfade / Konfiguration
# ---------------------------------------------------------------------------

def _base_dir() -> str:
    """Verzeichnis neben der EXE/dem Skript (funktioniert auch im PyInstaller-Bundle)."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

DEFAULT_CONFIG = {
    "mqtt_host": "0.0.0.0",
    "mqtt_port": 1883,
    "web_host": "0.0.0.0",
    "web_port": 8090,
    "allow_anonymous": True,
    "username": "",
    "password": "",
    "max_log_entries": 500,
    "client_timeout_seconds": 90,
}

logging.basicConfig(level=logging.WARNING, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("mqtt-monitor")


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception as exc:  # noqa: BLE001
            log.warning("config.json konnte nicht gelesen werden (%s), nutze Standardwerte", exc)
    else:
        save_config(cfg)
    return cfg


def save_config(cfg: dict) -> None:
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001
        log.warning("config.json konnte nicht geschrieben werden (%s)", exc)


# ---------------------------------------------------------------------------
# Zentraler, thread-sicherer Zustand
# ---------------------------------------------------------------------------

class AppState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.config = load_config()
        self.status = "stopped"          # stopped | starting | running | error
        self.error_message = ""
        self.started_at = None
        self.clients: dict[str, dict] = {}
        self.topics: dict[str, dict] = {}
        self.log: deque = deque(maxlen=self.config.get("max_log_entries", 500))
        self.flashes: deque = deque(maxlen=200)

    # -- Konfiguration -----------------------------------------------------
    def update_config(self, new_cfg: dict) -> None:
        with self.lock:
            self.config.update(new_cfg)
            self.log = deque(self.log, maxlen=self.config.get("max_log_entries", 500))

    # -- Broker-Lifecycle ----------------------------------------------------
    def set_status(self, status: str, error: str = "") -> None:
        with self.lock:
            self.status = status
            self.error_message = error
            if status == "running":
                self.started_at = time.time()
            if status in ("stopped", "error"):
                self.started_at = None

    def reset_traffic(self) -> None:
        with self.lock:
            self.clients.clear()
            self.topics.clear()
            self.log.clear()
            self.flashes.clear()

    # -- Ereignisse aus dem Broker-Plugin ------------------------------------
    def on_connect(self, client_id: str, address: str) -> None:
        with self.lock:
            now = time.time()
            self.clients[client_id] = {
                "id": client_id,
                "address": address or "?",
                "connected_at": now,
                "last_seen": now,
                "subs": {},
                "msg_count": 0,
            }
            self._append_log("connect", client_id, None, None, None)

    def on_disconnect(self, client_id: str) -> None:
        with self.lock:
            self.clients.pop(client_id, None)
            for topic in self.topics.values():
                topic["subscribers"].discard(client_id)
            self._append_log("disconnect", client_id, None, None, None)

    def on_subscribe(self, client_id: str, topic_filter: str, qos: int) -> None:
        with self.lock:
            client = self.clients.get(client_id)
            if client is not None:
                client["subs"][topic_filter] = qos
                client["last_seen"] = time.time()
            topic = self._ensure_topic(topic_filter)
            topic["subscribers"].add(client_id)
            self._append_log("subscribe", client_id, topic_filter, None, qos)

    def on_unsubscribe(self, client_id: str, topic_filter: str) -> None:
        with self.lock:
            client = self.clients.get(client_id)
            if client is not None:
                client["subs"].pop(topic_filter, None)
                client["last_seen"] = time.time()
            topic = self.topics.get(topic_filter)
            if topic is not None:
                topic["subscribers"].discard(client_id)
            self._append_log("unsubscribe", client_id, topic_filter, None, None)

    def on_message(self, client_id: str, topic_name: str, data: bytes, qos: int) -> None:
        with self.lock:
            now = time.time()
            client = self.clients.get(client_id)
            if client is not None:
                client["msg_count"] += 1
                client["last_seen"] = now
            topic = self._ensure_topic(topic_name)
            topic["publishers"].add(client_id)
            topic["count"] += 1
            topic["last_qos"] = qos
            topic["last_client"] = client_id
            topic["last_seen"] = now
            topic["last_payload"] = self._preview_payload(data)
            self.flashes.append({"client": client_id, "topic": topic_name, "ts": now})
            self._append_log("message", client_id, topic_name, topic["last_payload"], qos)

    # -- interne Helfer -------------------------------------------------------
    def _ensure_topic(self, name: str) -> dict:
        if name not in self.topics:
            self.topics[name] = {
                "name": name,
                "count": 0,
                "last_qos": None,
                "last_client": None,
                "last_seen": None,
                "last_payload": "",
                "publishers": set(),
                "subscribers": set(),
            }
        return self.topics[name]

    @staticmethod
    def _preview_payload(data: bytes, limit: int = 160) -> str:
        try:
            text = bytes(data).decode("utf-8")
            if not text.isprintable() and any(ord(c) < 9 for c in text):
                raise ValueError
        except Exception:  # noqa: BLE001
            text = bytes(data).hex(" ")
            if len(text) > limit:
                text = text[:limit] + "…"
            return f"0x{text}"
        if len(text) > limit:
            text = text[:limit] + "…"
        return text

    def _append_log(self, type_: str, client_id, topic, payload, qos) -> None:
        self.log.append(
            {
                "ts": time.time(),
                "type": type_,
                "client": client_id,
                "topic": topic,
                "payload": payload,
                "qos": qos,
            }
        )

    def _prune_stale_clients(self) -> None:
        timeout = self.config.get("client_timeout_seconds", 90)
        now = time.time()
        with self.lock:
            stale = [cid for cid, c in self.clients.items() if now - c["last_seen"] > timeout]
            for cid in stale:
                self.on_disconnect(cid)

    # -- Snapshot fuer die Web-UI ---------------------------------------------
    def snapshot(self) -> dict:
        self._prune_stale_clients()
        with self.lock:
            now = time.time()
            uptime = (now - self.started_at) if self.started_at else 0
            recent_flashes = [f for f in self.flashes if now - f["ts"] < 3.0]
            clients_out = [
                {
                    "id": c["id"],
                    "address": c["address"],
                    "connected_at": c["connected_at"],
                    "last_seen": c["last_seen"],
                    "subs": list(c["subs"].keys()),
                    "msg_count": c["msg_count"],
                }
                for c in sorted(self.clients.values(), key=lambda x: x["connected_at"])
            ]
            topics_out = [
                {
                    "name": t["name"],
                    "count": t["count"],
                    "last_qos": t["last_qos"],
                    "last_client": t["last_client"],
                    "last_seen": t["last_seen"],
                    "last_payload": t["last_payload"],
                    "publishers": sorted(t["publishers"]),
                    "subscribers": sorted(t["subscribers"]),
                }
                for t in sorted(self.topics.values(), key=lambda x: -(x["last_seen"] or 0))
            ]
            log_out = list(self.log)[-200:][::-1]
            return {
                "version": VERSION,
                "status": self.status,
                "error": self.error_message,
                "uptime": uptime,
                "mqtt_host": self.config.get("mqtt_host"),
                "mqtt_port": self.config.get("mqtt_port"),
                "web_port": self.config.get("web_port"),
                "allow_anonymous": self.config.get("allow_anonymous"),
                "clients": clients_out,
                "topics": topics_out,
                "log": log_out,
                "flashes": recent_flashes,
            }


STATE = AppState()


# ---------------------------------------------------------------------------
# amqtt-Plugins (muessen ueber Dotted-Path ladbar sein -> Modul "__main__")
# ---------------------------------------------------------------------------

class AuthPlugin(BaseAuthPlugin):
    """Erlaubt anonyme Verbindungen ODER prueft Benutzername/Passwort aus config.json."""

    @dataclass
    class Config:
        pass

    async def authenticate(self, *, session) -> bool:  # noqa: ANN001
        cfg = STATE.config
        if cfg.get("allow_anonymous", True):
            return True
        expected_user = cfg.get("username") or ""
        expected_pass = cfg.get("password") or ""
        if not expected_user:
            return True
        return session.username == expected_user and session.password == expected_pass


class MonitorPlugin(BasePlugin):
    """Spiegelt alle Broker-Ereignisse in den gemeinsamen AppState (STATE)."""

    @dataclass
    class Config:
        pass

    async def on_broker_client_connected(self, *, client_id, client_session) -> None:  # noqa: ANN001
        STATE.on_connect(client_id, getattr(client_session, "remote_address", None))

    async def on_broker_client_disconnected(self, *, client_id, client_session) -> None:  # noqa: ANN001
        STATE.on_disconnect(client_id)

    async def on_broker_client_subscribed(self, *, client_id, topic, qos) -> None:  # noqa: ANN001
        STATE.on_subscribe(client_id, topic, qos)

    async def on_broker_client_unsubscribed(self, *, client_id, topic) -> None:  # noqa: ANN001
        STATE.on_unsubscribe(client_id, topic)

    async def on_broker_message_received(self, *, client_id, message) -> None:  # noqa: ANN001
        STATE.on_message(client_id, message.topic, bytes(message.data), message.qos)


# ---------------------------------------------------------------------------
# Broker in eigenem Thread + eigenem asyncio-Loop
# ---------------------------------------------------------------------------

class BrokerRunner:
    def __init__(self) -> None:
        self.thread: threading.Thread | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.broker: Broker | None = None
        self._stop_flag = threading.Event()

    def start(self) -> None:
        self._stop_flag = threading.Event()
        self.thread = threading.Thread(target=self._run, name="mqtt-broker", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self._stop_flag.set()
        if self.thread is not None:
            self.thread.join(timeout=8)

    def _run(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:  # noqa: BLE001
            log.exception("Broker-Thread abgebrochen")
            STATE.set_status("error", str(exc))
        finally:
            self.loop.close()

    async def _main(self) -> None:
        cfg = STATE.config
        bind = f"{cfg['mqtt_host']}:{cfg['mqtt_port']}"
        amqtt_config = {
            "listeners": {"default": {"type": "tcp", "bind": bind}},
            "plugins": {
                "__main__.AuthPlugin": {},
                "__main__.MonitorPlugin": {},
            },
        }
        STATE.set_status("starting")
        try:
            self.broker = Broker(amqtt_config)
            await self.broker.start()
        except Exception as exc:  # noqa: BLE001
            log.exception("Broker konnte nicht gestartet werden")
            STATE.set_status("error", str(exc))
            return
        STATE.set_status("running")
        log.info("MQTT-Broker laeuft auf %s", bind)
        try:
            while not self._stop_flag.is_set():
                await asyncio.sleep(0.25)
        finally:
            try:
                await self.broker.shutdown()
            except Exception:  # noqa: BLE001
                pass
            STATE.set_status("stopped")


RUNNER = BrokerRunner()


# ---------------------------------------------------------------------------
# Flask-Anwendung
# ---------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/")
def index() -> Response:
    return Response(INDEX_HTML.replace("__VERSION__", VERSION), mimetype="text/html")


@app.route("/api/state")
def api_state():
    return jsonify(STATE.snapshot())


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "GET":
        return jsonify(STATE.config)

    data = request.get_json(force=True, silent=True) or {}
    allowed = {
        "mqtt_host", "mqtt_port", "web_host", "web_port",
        "allow_anonymous", "username", "password",
        "max_log_entries", "client_timeout_seconds",
    }
    new_cfg = {k: v for k, v in data.items() if k in allowed}
    restart_needed = any(k in new_cfg for k in ("mqtt_host", "mqtt_port", "allow_anonymous", "username", "password"))

    STATE.update_config(new_cfg)
    save_config(STATE.config)

    if restart_needed:
        RUNNER.stop()
        STATE.reset_traffic()
        RUNNER.start()

    return jsonify({"ok": True, "config": STATE.config, "restarted": restart_needed})


@app.route("/api/clear", methods=["POST"])
def api_clear():
    STATE.reset_traffic()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Frontend (eingebettetes HTML/CSS/JS, dunkles Anduril-Lattice-Theme)
# ---------------------------------------------------------------------------

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MQTT Monitor Dashboard</title>
<style>
:root{
  --bg:#0a0d0e; --panel:#10161a; --panel-2:#141c20; --border:#223038;
  --amber:#ffb020; --amber-dim:#8a5c14; --teal:#2dd4bf; --teal-dim:#1c6f66;
  --text:#dfe8ea; --text-dim:#7d939b; --danger:#ff5d5d; --ok:#3ddc84;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:'Consolas','SF Mono',ui-monospace,monospace;
  background-image:
    linear-gradient(rgba(45,212,191,0.035) 1px, transparent 1px),
    linear-gradient(90deg, rgba(45,212,191,0.035) 1px, transparent 1px);
  background-size:26px 26px;}
header{display:flex;align-items:center;justify-content:space-between;padding:14px 22px;
  border-bottom:1px solid var(--border);background:linear-gradient(180deg,#0d1315,#0a0d0e);}
header h1{font-size:16px;letter-spacing:2px;text-transform:uppercase;margin:0;color:var(--amber);font-weight:600;}
header h1 small{color:var(--text-dim);font-weight:400;letter-spacing:1px;margin-left:10px;font-size:11px;}
.badge{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border:1px solid var(--border);
  border-radius:2px;font-size:11px;letter-spacing:1px;text-transform:uppercase;background:var(--panel);}
.dot{width:7px;height:7px;border-radius:50%;background:var(--text-dim);}
.dot.running{background:var(--ok);box-shadow:0 0 6px var(--ok);}
.dot.error{background:var(--danger);box-shadow:0 0 6px var(--danger);}
.dot.starting,.dot.stopped{background:var(--amber);box-shadow:0 0 6px var(--amber);}
.toolbar{display:flex;gap:10px;align-items:center;}
button{background:var(--panel-2);border:1px solid var(--border);color:var(--text);
  padding:7px 12px;font-size:11px;letter-spacing:1px;text-transform:uppercase;cursor:pointer;border-radius:2px;
  font-family:inherit;transition:border-color .15s,color .15s;}
button:hover{border-color:var(--teal);color:var(--teal);}
button.primary{border-color:var(--amber-dim);color:var(--amber);}
button.primary:hover{border-color:var(--amber);}
main{padding:18px 22px;display:flex;flex-direction:column;gap:16px;max-width:1400px;margin:0 auto;}
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;}
.stat{background:var(--panel);border:1px solid var(--border);padding:10px 14px;border-radius:2px;}
.stat .label{font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:var(--text-dim);}
.stat .value{font-size:20px;color:var(--teal);margin-top:2px;}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:3px;overflow:hidden;}
.panel-head{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;
  border-bottom:1px solid var(--border);background:var(--panel-2);}
.panel-head h2{font-size:12px;letter-spacing:1.5px;text-transform:uppercase;margin:0;color:var(--text-dim);}
#graph-wrap{position:relative;}
#graph{display:block;width:100%;height:440px;}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px;}
table{width:100%;border-collapse:collapse;font-size:12px;}
th{text-align:left;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;font-size:10px;
  padding:8px 12px;border-bottom:1px solid var(--border);}
td{padding:6px 12px;border-bottom:1px solid rgba(34,48,56,0.5);color:var(--text);}
tr:hover td{background:rgba(45,212,191,0.05);}
.tag{display:inline-block;padding:1px 6px;border-radius:2px;font-size:10px;letter-spacing:0.5px;text-transform:uppercase;}
.tag.message{background:rgba(255,176,32,0.15);color:var(--amber);}
.tag.connect{background:rgba(61,220,132,0.15);color:var(--ok);}
.tag.disconnect{background:rgba(255,93,93,0.15);color:var(--danger);}
.tag.subscribe,.tag.unsubscribe{background:rgba(45,212,191,0.15);color:var(--teal);}
.mono-dim{color:var(--text-dim);}
.log-scroll{max-height:340px;overflow-y:auto;}
.cfg-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px 16px;padding:16px;}
.cfg-grid label{display:flex;flex-direction:column;gap:5px;font-size:11px;color:var(--text-dim);
  text-transform:uppercase;letter-spacing:1px;}
.cfg-grid input[type=text],.cfg-grid input[type=number],.cfg-grid input[type=password]{
  background:var(--bg);border:1px solid var(--border);color:var(--text);padding:7px 9px;border-radius:2px;
  font-family:inherit;font-size:13px;}
.cfg-grid .row-full{grid-column:1/-1;display:flex;gap:16px;align-items:center;justify-content:flex-end;padding-top:6px;}
.checkbox-row{flex-direction:row !important;align-items:center;gap:8px !important;}
.hidden{display:none !important;}
.empty{padding:22px;text-align:center;color:var(--text-dim);font-size:12px;}
footer{text-align:center;color:var(--text-dim);font-size:11px;padding:20px;letter-spacing:1px;}
</style>
</head>
<body>
<header>
  <h1>MQTT Monitor Dashboard <small>v__VERSION__</small></h1>
  <div class="toolbar">
    <span class="badge"><span class="dot" id="status-dot"></span><span id="status-text">-</span></span>
    <span class="badge" id="bind-badge">-</span>
    <button id="btn-config">Konfiguration</button>
    <button id="btn-clear">Log leeren</button>
  </div>
</header>
<main>

  <div class="stats">
    <div class="stat"><div class="label">Broker</div><div class="value" id="stat-bind">-</div></div>
    <div class="stat"><div class="label">Laufzeit</div><div class="value" id="stat-uptime">-</div></div>
    <div class="stat"><div class="label">Clients</div><div class="value" id="stat-clients">0</div></div>
    <div class="stat"><div class="label">Topics</div><div class="value" id="stat-topics">0</div></div>
    <div class="stat"><div class="label">Nachrichten/5s</div><div class="value" id="stat-rate">0</div></div>
  </div>

  <div class="panel" id="cfg-panel" style="display:none;">
    <div class="panel-head"><h2>Konfiguration</h2></div>
    <div class="cfg-grid">
      <label>MQTT Bind-Adresse <input type="text" id="cfg-mqtt-host"></label>
      <label>MQTT Port <input type="number" id="cfg-mqtt-port"></label>
      <label>Web Bind-Adresse <input type="text" id="cfg-web-host"></label>
      <label>Web Port <input type="number" id="cfg-web-port"></label>
      <label class="checkbox-row"><input type="checkbox" id="cfg-anon" style="width:auto;"> Anonyme Verbindungen erlauben</label>
      <label>Client-Timeout (s) <input type="number" id="cfg-timeout"></label>
      <label>Benutzername <input type="text" id="cfg-user"></label>
      <label>Passwort <input type="password" id="cfg-pass"></label>
      <div class="row-full">
        <span class="mono-dim" id="cfg-hint">Aenderungen an Bind-Adresse/Port/Zugangsdaten starten den Broker neu.</span>
        <button class="primary" id="cfg-save">Speichern</button>
      </div>
    </div>
  </div>

  <div class="panel" id="graph-wrap">
    <div class="panel-head"><h2>MQTT-Diagramm — Live-Knotengraph</h2>
      <span class="mono-dim" style="font-size:11px;">Broker · Clients · Topics</span></div>
    <canvas id="graph"></canvas>
  </div>

  <div class="grid2">
    <div class="panel">
      <div class="panel-head"><h2>Clients</h2></div>
      <div class="log-scroll">
        <table><thead><tr><th>ID</th><th>Adresse</th><th>Subs</th><th>Msgs</th><th>Seit</th></tr></thead>
        <tbody id="tbl-clients"></tbody></table>
      </div>
    </div>
    <div class="panel">
      <div class="panel-head"><h2>Topics</h2></div>
      <div class="log-scroll">
        <table><thead><tr><th>Topic</th><th>Letzter Wert</th><th>QoS</th><th>Anzahl</th></tr></thead>
        <tbody id="tbl-topics"></tbody></table>
      </div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-head"><h2>Paket-Log</h2></div>
    <div class="log-scroll">
      <table><thead><tr><th>Zeit</th><th>Typ</th><th>Client</th><th>Topic</th><th>Payload</th><th>QoS</th></tr></thead>
      <tbody id="tbl-log"></tbody></table>
    </div>
  </div>

</main>
<footer>MQTT-Monitor-Dashboard-MK1 · v__VERSION__</footer>

<script>
const canvas = document.getElementById('graph');
const ctx = canvas.getContext('2d');
let latestData = null;
let localFlashes = []; // {client, topic, seenAt, key}
let seenFlashKeys = new Set();
let rateHistory = []; // timestamps of messages seen client-side

function resizeCanvas(){
  const rect = canvas.parentElement.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = rect.width * dpr;
  canvas.height = 440 * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = '440px';
  ctx.setTransform(dpr,0,0,dpr,0,0);
}
window.addEventListener('resize', resizeCanvas);

function fmtTime(ts){
  if(!ts) return '-';
  const d = new Date(ts*1000);
  return d.toLocaleTimeString('de-DE', {hour12:false});
}
function fmtUptime(sec){
  sec = Math.floor(sec||0);
  const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60), s = sec%60;
  return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');
}
function esc(s){
  if(s===null||s===undefined) return '';
  return String(s).replace(/[&<>"]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
}

async function poll(){
  try{
    const res = await fetch('/api/state', {cache:'no-store'});
    const data = await res.json();
    applyData(data);
  }catch(e){ /* still trying */ }
  setTimeout(poll, 700);
}

function applyData(data){
  latestData = data;

  document.getElementById('status-text').textContent = data.status.toUpperCase();
  const dot = document.getElementById('status-dot');
  dot.className = 'dot ' + data.status;
  document.getElementById('bind-badge').textContent = data.mqtt_host + ':' + data.mqtt_port;
  document.getElementById('stat-bind').textContent = data.mqtt_host + ':' + data.mqtt_port;
  document.getElementById('stat-uptime').textContent = fmtUptime(data.uptime);
  document.getElementById('stat-clients').textContent = data.clients.length;
  document.getElementById('stat-topics').textContent = data.topics.length;

  // neue Flashes uebernehmen
  const now = performance.now();
  for(const f of data.flashes){
    const key = f.client+'|'+f.topic+'|'+f.ts;
    if(!seenFlashKeys.has(key)){
      seenFlashKeys.add(key);
      localFlashes.push({client:f.client, topic:f.topic, seenAt:now});
      rateHistory.push(Date.now());
    }
  }
  if(seenFlashKeys.size > 4000){ seenFlashKeys = new Set(Array.from(seenFlashKeys).slice(-1000)); }
  const cutoff = Date.now()-5000;
  rateHistory = rateHistory.filter(t=>t>cutoff);
  document.getElementById('stat-rate').textContent = rateHistory.length;

  renderClients(data.clients);
  renderTopics(data.topics);
  renderLog(data.log);
}

function renderClients(clients){
  const tbody = document.getElementById('tbl-clients');
  if(clients.length===0){ tbody.innerHTML = '<tr><td colspan="5" class="empty">Keine Clients verbunden</td></tr>'; return; }
  tbody.innerHTML = clients.map(c => `
    <tr>
      <td>${esc(c.id)}</td>
      <td class="mono-dim">${esc(c.address)}</td>
      <td>${c.subs.length}</td>
      <td>${c.msg_count}</td>
      <td class="mono-dim">${fmtTime(c.connected_at)}</td>
    </tr>`).join('');
}

function renderTopics(topics){
  const tbody = document.getElementById('tbl-topics');
  if(topics.length===0){ tbody.innerHTML = '<tr><td colspan="4" class="empty">Noch keine Topics gesehen</td></tr>'; return; }
  tbody.innerHTML = topics.map(t => `
    <tr>
      <td>${esc(t.name)}</td>
      <td class="mono-dim">${esc(t.last_payload)}</td>
      <td>${t.last_qos ?? '-'}</td>
      <td>${t.count}</td>
    </tr>`).join('');
}

function renderLog(entries){
  const tbody = document.getElementById('tbl-log');
  if(entries.length===0){ tbody.innerHTML = '<tr><td colspan="6" class="empty">Noch keine Pakete</td></tr>'; return; }
  tbody.innerHTML = entries.map(e => `
    <tr>
      <td class="mono-dim">${fmtTime(e.ts)}</td>
      <td><span class="tag ${e.type}">${e.type}</span></td>
      <td>${esc(e.client)}</td>
      <td>${esc(e.topic)||''}</td>
      <td class="mono-dim">${esc(e.payload)||''}</td>
      <td>${e.qos ?? ''}</td>
    </tr>`).join('');
}

// ---- Graph-Rendering ----
function draw(){
  requestAnimationFrame(draw);
  if(!latestData) return;
  const rect = canvas.getBoundingClientRect();
  const w = rect.width, h = 440;
  ctx.clearRect(0,0,w,h);

  const cx = w/2, cy = h/2;
  const clients = latestData.clients;
  const topics = latestData.topics;
  const rClient = Math.min(w,h)*0.28;
  const rTopic = Math.min(w,h)*0.46;

  const clientPos = {};
  clients.forEach((c,i)=>{
    const a = (i/Math.max(clients.length,1))*Math.PI*2 - Math.PI/2;
    clientPos[c.id] = {x: cx+Math.cos(a)*rClient, y: cy+Math.sin(a)*rClient, angle:a};
  });
  const topicPos = {};
  topics.forEach((t,i)=>{
    const a = (i/Math.max(topics.length,1))*Math.PI*2 - Math.PI/2 + 0.15;
    topicPos[t.name] = {x: cx+Math.cos(a)*rTopic, y: cy+Math.sin(a)*rTopic};
  });

  const now = performance.now();
  localFlashes = localFlashes.filter(f => now-f.seenAt < 900);
  const flashMap = {}; // "client|topic" -> age 0..1 (1=fresh)
  for(const f of localFlashes){
    const age = 1 - (now-f.seenAt)/900;
    const key = f.client+'|'+f.topic;
    flashMap[key] = Math.max(flashMap[key]||0, age);
  }

  // Kanten: Client <-> Broker
  ctx.lineWidth = 1;
  clients.forEach(c=>{
    const p = clientPos[c.id];
    ctx.strokeStyle = 'rgba(45,212,191,0.25)';
    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.lineTo(p.x,p.y); ctx.stroke();
  });

  // Kanten: Client <-> Topic (subscribe = teal, publish-flash = amber)
  topics.forEach(t=>{
    const tp = topicPos[t.name];
    const related = new Set([...(t.subscribers||[]), ...(t.publishers||[])]);
    related.forEach(cid=>{
      const cp = clientPos[cid];
      if(!cp) return;
      const key = cid+'|'+t.name;
      const flash = flashMap[key]||0;
      ctx.strokeStyle = flash>0 ? `rgba(255,176,32,${0.35+0.6*flash})` : 'rgba(45,212,191,0.15)';
      ctx.lineWidth = flash>0 ? 1.5+2.5*flash : 1;
      ctx.beginPath(); ctx.moveTo(cp.x,cp.y); ctx.lineTo(tp.x,tp.y); ctx.stroke();
    });
  });

  // Broker-Knoten
  ctx.beginPath(); ctx.arc(cx,cy,26,0,Math.PI*2);
  ctx.fillStyle = latestData.status==='running' ? 'rgba(255,176,32,0.18)' : 'rgba(255,93,93,0.15)';
  ctx.fill();
  ctx.lineWidth = 2; ctx.strokeStyle = latestData.status==='running' ? '#ffb020' : '#ff5d5d'; ctx.stroke();
  ctx.fillStyle = '#ffb020'; ctx.font = '10px monospace'; ctx.textAlign='center'; ctx.textBaseline='middle';
  ctx.fillText('BROKER', cx, cy);

  // Client-Knoten
  clients.forEach(c=>{
    const p = clientPos[c.id];
    let flash = 0;
    Object.keys(flashMap).forEach(k=>{ if(k.startsWith(c.id+'|')) flash = Math.max(flash, flashMap[k]); });
    const r = 12 + 4*flash;
    ctx.beginPath(); ctx.arc(p.x,p.y,r,0,Math.PI*2);
    ctx.fillStyle = flash>0 ? `rgba(255,176,32,${0.3+0.4*flash})` : 'rgba(45,212,191,0.15)';
    ctx.fill();
    ctx.lineWidth = 1.5; ctx.strokeStyle = flash>0 ? '#ffb020' : '#2dd4bf'; ctx.stroke();
    ctx.fillStyle = '#dfe8ea'; ctx.font='9px monospace'; ctx.textAlign='center'; ctx.textBaseline='top';
    const label = c.id.length>14 ? c.id.slice(0,12)+'…' : c.id;
    ctx.fillText(label, p.x, p.y+r+3);
  });

  // Topic-Knoten
  topics.forEach(t=>{
    const p = topicPos[t.name];
    let flash = 0;
    Object.keys(flashMap).forEach(k=>{ if(k.endsWith('|'+t.name)) flash = Math.max(flash, flashMap[k]); });
    const r = 6 + Math.min(10, Math.log2((t.count||1)+1)) + 3*flash;
    ctx.beginPath(); ctx.arc(p.x,p.y,r,0,Math.PI*2);
    ctx.fillStyle = flash>0 ? `rgba(255,176,32,${0.35+0.5*flash})` : 'rgba(255,255,255,0.06)';
    ctx.fill();
    ctx.lineWidth = 1; ctx.strokeStyle = flash>0 ? '#ffb020' : '#5a6f77'; ctx.stroke();
    ctx.fillStyle = '#8fa3aa'; ctx.font='9px monospace'; ctx.textAlign='center'; ctx.textBaseline='top';
    const label = t.name.length>18 ? t.name.slice(0,16)+'…' : t.name;
    ctx.fillText(label, p.x, p.y+r+3);
  });
}

// ---- Konfigurationspanel ----
const cfgPanel = document.getElementById('cfg-panel');
document.getElementById('btn-config').addEventListener('click', async ()=>{
  if(cfgPanel.style.display==='none'){
    const res = await fetch('/api/config'); const cfg = await res.json();
    document.getElementById('cfg-mqtt-host').value = cfg.mqtt_host;
    document.getElementById('cfg-mqtt-port').value = cfg.mqtt_port;
    document.getElementById('cfg-web-host').value = cfg.web_host;
    document.getElementById('cfg-web-port').value = cfg.web_port;
    document.getElementById('cfg-anon').checked = !!cfg.allow_anonymous;
    document.getElementById('cfg-timeout').value = cfg.client_timeout_seconds;
    document.getElementById('cfg-user').value = cfg.username||'';
    document.getElementById('cfg-pass').value = cfg.password||'';
    cfgPanel.style.display = 'block';
  } else {
    cfgPanel.style.display = 'none';
  }
});
document.getElementById('cfg-save').addEventListener('click', async ()=>{
  const body = {
    mqtt_host: document.getElementById('cfg-mqtt-host').value.trim(),
    mqtt_port: parseInt(document.getElementById('cfg-mqtt-port').value,10),
    web_host: document.getElementById('cfg-web-host').value.trim(),
    web_port: parseInt(document.getElementById('cfg-web-port').value,10),
    allow_anonymous: document.getElementById('cfg-anon').checked,
    client_timeout_seconds: parseInt(document.getElementById('cfg-timeout').value,10),
    username: document.getElementById('cfg-user').value,
    password: document.getElementById('cfg-pass').value,
  };
  const res = await fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
  const out = await res.json();
  alert(out.restarted ? 'Gespeichert. Broker wird neu gestartet.' : 'Gespeichert.');
});

document.getElementById('btn-clear').addEventListener('click', async ()=>{
  await fetch('/api/clear', {method:'POST'});
});

resizeCanvas();
poll();
draw();
</script>
</body>
</html>
"""


def main() -> None:
    RUNNER.start()
    web_host = STATE.config.get("web_host", "0.0.0.0")
    web_port = STATE.config.get("web_port", 8090)
    log.info("Web-UI auf http://%s:%s", web_host, web_port)
    app.run(host=web_host, port=web_port, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
