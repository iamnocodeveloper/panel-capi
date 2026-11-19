#!/usr/bin/env python3
"""Panel de métricas de la flota Capi.

Servidor HTTP autocontenido (solo stdlib) que:
  - sirve el dashboard (index.html),
  - mide capi-ops (/health) cada 60 s y guarda un histórico rodante en memoria + disco,
  - barre el inventario de sesiones WAHA de la flota cada 20 min en segundo plano,
  - expone GET /api/data y GET /api/health protegidos con HTTP Basic.

No escribe ni registra credenciales en ningún log.
"""
import base64
import hmac
import json
import os
import re
import ssl
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------- config
AQUI = os.path.dirname(os.path.abspath(__file__))
PANEL_USER = os.environ.get("PANEL_USER", "").strip()
PANEL_PASS = os.environ.get("PANEL_PASS", "").strip()
WAHA_API_KEY = os.environ.get("WAHA_API_KEY", "").strip()

OPS_HEALTH = os.environ.get("OPS_HEALTH_URL", "http://207.231.110.162:18080/health").strip()
OPS_METRICS = os.environ.get("OPS_METRICS_URL", "http://207.231.110.162:18080/metrics").strip()
PUERTO = int(os.environ.get("PORT", "8080"))
PASO = 60                 # segundos entre puntos del histórico
MAX_PUNTOS = 480          # ~8 h de histórico rodante
BARRIDO_S = 1200          # cada 20 min el inventario de sesiones
HILOS_WAHA = 20
TIMEOUT_WAHA = 15
TIMEOUT_OPS = 12

DATA_DIR = os.environ.get("DATA_DIR", "/data")
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    _probe = os.path.join(DATA_DIR, ".w")
    with open(_probe, "w") as _f:
        _f.write("1")
    os.remove(_probe)
except Exception:
    DATA_DIR = os.path.join(AQUI, "data")
    os.makedirs(DATA_DIR, exist_ok=True)

F_HIST = os.path.join(DATA_DIR, "historico.json")
F_SES = os.path.join(DATA_DIR, "sesiones.json")

# Rangos declarados de la flota
RANGOS = [("capi1", 2, 17), ("capi2", 18, 28), ("capi3", 29, 45), ("capi4", 46, 60),
          ("capi5", 61, 74), ("capi6", 75, 84), ("capi7", 85, 94), ("capi8", 95, 104),
          ("capi9", 105, 114)]
ORDEN = [r[0] for r in RANGOS] + ["capi-20.45"]
HOSTS_WAHA = ([f"waha{n}.supercrm.best" for n in
               list(range(2, 29)) + list(range(29, 46)) + list(range(46, 61)) +
               list(range(61, 69)) + list(range(75, 85)) + list(range(85, 95)) +
               list(range(105, 115))]
              + ["blackbet.supercrm.best", "wahacapi.supercrm.best"])

CTX = ssl.create_default_context()

LOCK = threading.Lock()
ESTADO = {
    "salud": None,          # último JSON de /health
    "salud_error": None,
    "medido_en": None,
    "puntos": [],           # [{"t": epoch, "s": {alias: [cpu, mem, disco]}}]
    "sesiones": {
        "actualizadoEn": None,
        "barriendo": False,
        "error": None,
        "hosts": [],        # [{"host","alias","ok","http","sesiones":[...]}]
    },
}


def log(msg):
    print("[panel-capi] %s %s" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg), flush=True)


def ahora_iso(t=None):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t if t is not None else time.time()))


# --------------------------------------------------------------------------- utilidades
def alias_de(host):
    if not host:
        return "otros"
    if host.startswith("blackbet"):
        return "capi8"
    if host.startswith("wahacapi"):
        return "capi-20.45"
    m = re.match(r"waha(\d+)\.", host)
    if m:
        n = int(m.group(1))
        for a, lo, hi in RANGOS:
            if lo <= n <= hi:
                return a
    return "otros"


def get_json(url, cabeceras=None, timeout=TIMEOUT_OPS):
    req = urllib.request.Request(url, headers=cabeceras or {})
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def guardar_json_atomico(ruta, datos):
    tmp = ruta + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, ruta)


# --------------------------------------------------------------------------- histórico
def cargar_disco():
    try:
        if os.path.exists(F_HIST):
            d = json.load(open(F_HIST, encoding="utf-8"))
            pts = d.get("puntos") or []
            with LOCK:
                ESTADO["puntos"] = pts[-MAX_PUNTOS:]
            log("histórico cargado: %d puntos" % len(pts))
    except Exception as e:
        log("no se pudo cargar el histórico: %s" % e)
    try:
        if os.path.exists(F_SES):
            d = json.load(open(F_SES, encoding="utf-8"))
            with LOCK:
                ESTADO["sesiones"]["hosts"] = d.get("hosts") or []
                ESTADO["sesiones"]["actualizadoEn"] = d.get("actualizadoEn")
            log("inventario de sesiones cargado de disco")
    except Exception as e:
        log("no se pudo cargar el inventario: %s" % e)


def medir():
    """Lee capi-ops y agrega un punto al histórico."""
    t = time.time()
    try:
        salud = get_json(OPS_HEALTH)
        if not isinstance(salud, dict) or "servers" not in salud:
            raise ValueError("respuesta de /health sin 'servers'")
    except Exception as e:
        with LOCK:
            ESTADO["salud_error"] = "%s: %s" % (type(e).__name__, e)
        log("fallo al leer capi-ops: %s" % ESTADO["salud_error"])
        return False

    punto = {"t": int(t), "s": {}}
    for s in salud.get("servers") or []:
        a = s.get("alias")
        if not a:
            continue
        punto["s"][a] = [s.get("cpu"), s.get("mem"), s.get("disk")]

    with LOCK:
        ESTADO["salud"] = salud
        ESTADO["salud_error"] = None
        ESTADO["medido_en"] = int(t)
        pts = ESTADO["puntos"]
        if pts and t - pts[-1]["t"] < PASO * 0.5:
            pts[-1] = punto
        else:
            pts.append(punto)
        if len(pts) > MAX_PUNTOS:
            del pts[: len(pts) - MAX_PUNTOS]
        copia = list(pts)
    try:
        guardar_json_atomico(F_HIST, {"guardadoEn": ahora_iso(), "paso": PASO, "puntos": copia})
    except Exception as e:
        log("no se pudo guardar el histórico: %s" % e)
    return True


def bucle_medicion():
    while True:
        t0 = time.time()
        try:
            medir()
        except Exception as e:
            log("error en el ciclo de medición: %s" % e)
        time.sleep(max(5.0, PASO - (time.time() - t0)))


# --------------------------------------------------------------------------- WAHA
def sesiones_host(host, key):
    url = "https://%s/api/sessions?all=true" % host
    req = urllib.request.Request(url, headers={"X-Api-Key": key, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_WAHA, context=CTX) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
            http = r.status
    except urllib.error.HTTPError as e:
        return {"host": host, "alias": alias_de(host), "ok": False, "http": e.code, "sesiones": []}
    except Exception as e:
        return {"host": host, "alias": alias_de(host), "ok": False, "http": 0,
                "sesiones": [], "error": type(e).__name__}
    if isinstance(data, dict):
        data = [data]
    filas = []
    for s in data or []:
        me = s.get("me") or {}
        filas.append({
            "name": s.get("name") or host.split(".")[0],
            "status": (s.get("status") or "").upper() or "SIN_ESTADO",
            "me": (me.get("id") or ""),
            "push": (me.get("pushName") or ""),
        })
    return {"host": host, "alias": alias_de(host), "ok": True, "http": http, "sesiones": filas}


def barrer_sesiones(key):
    with LOCK:
        if ESTADO["sesiones"]["barriendo"]:
            return
        ESTADO["sesiones"]["barriendo"] = True
    t0 = time.time()
    try:
        with ThreadPoolExecutor(max_workers=HILOS_WAHA) as ex:
            res = list(ex.map(lambda h: sesiones_host(h, key), HOSTS_WAHA))
        res.sort(key=lambda r: (ORDEN.index(r["alias"]) if r["alias"] in ORDEN else 99,
                                int(re.sub(r"\D", "", r["host"]) or 0), r["host"]))
        total = sum(len(r["sesiones"]) for r in res)
        caidas = sum(1 for r in res for s in r["sesiones"] if s["status"] == "FAILED")
        with LOCK:
            ESTADO["sesiones"]["hosts"] = res
            ESTADO["sesiones"]["actualizadoEn"] = int(t0)
            ESTADO["sesiones"]["error"] = None
        try:
            guardar_json_atomico(F_SES, {"actualizadoEn": int(t0), "hosts": res})
        except Exception as e:
            log("no se pudo guardar el inventario: %s" % e)
        log("barrido WAHA: %d instancias, %d sesiones, %d caídas, %.0fs"
            % (len(res), total, caidas, time.time() - t0))
    except Exception as e:
        with LOCK:
            ESTADO["sesiones"]["error"] = "%s: %s" % (type(e).__name__, e)
        log("error en el barrido WAHA: %s" % e)
    finally:
        with LOCK:
            ESTADO["sesiones"]["barriendo"] = False


def bucle_sesiones():
    while True:
        if WAHA_API_KEY:
            try:
                barrer_sesiones(WAHA_API_KEY)
            except Exception as e:
                log("error en el hilo de sesiones: %s" % e)
        else:
            log("sin WAHA_API_KEY: no se barren sesiones")
            return
        time.sleep(BARRIDO_S)


# --------------------------------------------------------------------------- agregación
def construir_datos():
    with LOCK:
        salud = ESTADO["salud"]
        salud_error = ESTADO["salud_error"]
        medido_en = ESTADO["medido_en"]
        puntos = list(ESTADO["puntos"])
        ses = {
            "actualizadoEn": ESTADO["sesiones"]["actualizadoEn"],
            "barriendo": ESTADO["sesiones"]["barriendo"],
            "error": ESTADO["sesiones"]["error"],
            "hosts": [dict(h) for h in ESTADO["sesiones"]["hosts"]],
        }

    # --- por servidor (sesiones)
    por_alias = {}
    for h in ses["hosts"]:
        a = h["alias"]
        d = por_alias.setdefault(a, {"instancias": 0, "instancias_ok": 0, "sesiones": 0,
                                     "working": 0, "failed": 0, "otros": 0,
                                     "numeros": set(), "estados": {}})
        d["instancias"] += 1
        if h.get("ok"):
            d["instancias_ok"] += 1
        for s in h.get("sesiones") or []:
            d["sesiones"] += 1
            st = s["status"]
            d["estados"][st] = d["estados"].get(st, 0) + 1
            if st == "WORKING":
                d["working"] += 1
            elif st == "FAILED":
                d["failed"] += 1
            else:
                d["otros"] += 1
            if s.get("me"):
                d["numeros"].add(s["me"])

    servidores = []
    totales = {"instancias": 0, "instancias_ok": 0, "sesiones": 0, "working": 0,
               "failed": 0, "otros": 0, "numeros": set(), "servidores": 0, "arriba": 0}
    vistos = set()
    for s in (salud or {}).get("servers") or []:
        a = s.get("alias")
        if not a:
            continue
        vistos.add(a)
        d = por_alias.get(a, {})
        item = {
            "alias": a,
            "ip": s.get("ip"),
            "reachable": bool(s.get("reachable")),
            "apps": s.get("apps"),
            "cpu": s.get("cpu"),
            "mem": s.get("mem"),
            "disk": s.get("disk"),
            "sesiones": {
                "instancias": d.get("instancias", 0),
                "instancias_ok": d.get("instancias_ok", 0),
                "sesiones": d.get("sesiones", 0),
                "working": d.get("working", 0),
                "failed": d.get("failed", 0),
                "otros": d.get("otros", 0),
                "numeros": len(d.get("numeros") or ()),
                "estados": d.get("estados", {}),
            },
        }
        servidores.append(item)
        totales["servidores"] += 1
        totales["arriba"] += 1 if item["reachable"] else 0
        totales["instancias"] += item["sesiones"]["instancias"]
        totales["instancias_ok"] += item["sesiones"]["instancias_ok"]
        totales["sesiones"] += item["sesiones"]["sesiones"]
        totales["working"] += item["sesiones"]["working"]
        totales["failed"] += item["sesiones"]["failed"]
        totales["otros"] += item["sesiones"]["otros"]
        totales["numeros"] |= set(d.get("numeros") or ())

    # servidores WAHA sin servidor de panel (wahacapi)
    for a, d in por_alias.items():
        if a in vistos:
            continue
        item = {"alias": a, "ip": None, "reachable": None, "apps": None,
                "cpu": None, "mem": None, "disk": None,
                "sesiones": {
                    "instancias": d["instancias"], "instancias_ok": d["instancias_ok"],
                    "sesiones": d["sesiones"], "working": d["working"],
                    "failed": d["failed"], "otros": d["otros"],
                    "numeros": len(d["numeros"]), "estados": d["estados"]}}
        servidores.append(item)
        vistos.add(a)
        totales["instancias"] += d["instancias"]
        totales["instancias_ok"] += d["instancias_ok"]
        totales["sesiones"] += d["sesiones"]
        totales["working"] += d["working"]
        totales["failed"] += d["failed"]
        totales["otros"] += d["otros"]
        totales["numeros"] |= d["numeros"]

    servidores.sort(key=lambda x: ORDEN.index(x["alias"]) if x["alias"] in ORDEN else 99)

    caidas = []
    for h in ses["hosts"]:
        for s in h.get("sesiones") or []:
            if s["status"] == "FAILED":
                caidas.append({"alias": h["alias"], "host": h["host"],
                               "nombre": s["name"], "estado": s["status"],
                               "push": s.get("push") or ""})
    caidas.sort(key=lambda c: (ORDEN.index(c["alias"]) if c["alias"] in ORDEN else 99, c["host"], c["nombre"]))

    sin_respuesta = [{"alias": h["alias"], "host": h["host"], "http": h.get("http")}
                     for h in ses["hosts"] if not h.get("ok")]

    edad_ses = int(time.time() - ses["actualizadoEn"]) if ses["actualizadoEn"] else None

    return {
        "generadoEn": ahora_iso(),
        "epoch": int(time.time()),
        "capi_ops": {
            "ok": salud is not None and salud.get("ok") is not False,
            "error": salud_error,
            "uptimeSeconds": (salud or {}).get("uptimeSeconds"),
            "lastCycleAt": (salud or {}).get("lastCycleAt"),
            "telegram": (salud or {}).get("telegram"),
            "telegramStatus": (salud or {}).get("telegramStatus"),
            "openIncidents": (salud or {}).get("openIncidents"),
            "easyPanel": (salud or {}).get("easyPanel"),
            "medidoEn": medido_en,
        },
        "kpis": {
            "servidores": totales["servidores"],
            "arriba": totales["arriba"],
            "instancias": totales["instancias"],
            "instanciasOk": totales["instancias_ok"],
            "sesiones": totales["sesiones"],
            "working": totales["working"],
            "failed": totales["failed"],
            "otros": totales["otros"],
            "numeros": len(totales["numeros"]),
            "incidencias": (salud or {}).get("openIncidents"),
        },
        "servidores": servidores,
        "sesiones": {
            "actualizadoEn": ses["actualizadoEn"],
            "edadSeg": edad_ses,
            "barriendo": ses["barriendo"],
            "error": ses["error"],
            "instanciasConsultadas": len(ses["hosts"]),
            "instanciasSinRespuesta": sin_respuesta,
            "caidas": caidas,
        },
        "historico": {"paso": PASO, "puntos": puntos},
    }


# --------------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "panel-capi/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # sin IPs ni credenciales en logs
        return

    def _autorizado(self):
        if not PANEL_USER or not PANEL_PASS:
            return False
        cab = self.headers.get("Authorization", "")
        if not cab.startswith("Basic "):
            return False
        try:
            user, _, pwd = base64.b64decode(cab[6:]).decode("utf-8").partition(":")
        except Exception:
            return False
        ok_u = hmac.compare_digest(user.encode(), PANEL_USER.encode())
        ok_p = hmac.compare_digest(pwd.encode(), PANEL_PASS.encode())
        return ok_u and ok_p

    def _texto(self, codigo, cuerpo, tipo="text/plain; charset=utf-8", extra=None):
        data = cuerpo.encode("utf-8") if isinstance(cuerpo, str) else cuerpo
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _json(self, codigo, obj):
        self._texto(codigo, json.dumps(obj, ensure_ascii=False),
                    "application/json; charset=utf-8", {"Cache-Control": "no-store"})

    def _no_autorizado(self):
        self._texto(401, "Autenticación requerida\n",
                    extra={"WWW-Authenticate": 'Basic realm="Panel Capi", charset="UTF-8"'})

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        ruta = self.path.split("?")[0]
        if ruta == "/favicon.ico":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if not self._autorizado():
            self._no_autorizado()
            return
        if ruta in ("/", "/index.html"):
            try:
                with open(os.path.join(AQUI, "index.html"), "rb") as f:
                    html = f.read()
            except Exception as e:
                self._texto(500, "No se pudo leer index.html: %s" % e)
                return
            self._texto(200, html, "text/html; charset=utf-8")
            return
        if ruta == "/api/data":
            self._json(200, construir_datos())
            return
        if ruta == "/api/health":
            with LOCK:
                ses_t = ESTADO["sesiones"]["actualizadoEn"]
                salud = ESTADO["salud"]
                barriendo = ESTADO["sesiones"]["barriendo"]
                error = ESTADO["sesiones"]["error"]
            edad = int(time.time() - ses_t) if ses_t else None
            ok = bool(salud) and (edad is None or edad < BARRIDO_S + 600)
            self._json(200, {
                "ok": ok,
                "edad_sesiones_seg": edad,
                "fuente_capi_ops": bool(salud),
                "capi_ops_ultimo_ciclo": (salud or {}).get("lastCycleAt"),
                "barriendo_sesiones": barriendo,
                "error_sesiones": error,
                "puntos_historico": len(ESTADO["puntos"]),
            })
            return
        if ruta == "/api/refresh":
            medir()
            if WAHA_API_KEY:
                with LOCK:
                    edad = int(time.time() - ESTADO["sesiones"]["actualizadoEn"]) if ESTADO["sesiones"]["actualizadoEn"] else 999999
                    ocupado = ESTADO["sesiones"]["barriendo"]
                if edad > 300 and not ocupado:
                    threading.Thread(target=barrer_sesiones, args=(WAHA_API_KEY,), daemon=True).start()
            self._json(200, construir_datos())
            return
        self._texto(404, "No encontrado\n")


def main():
    if not PANEL_USER or not PANEL_PASS:
        log("FATAL: faltan PANEL_USER / PANEL_PASS; no se sirve el panel sin autenticación")
        return 2
    cargar_disco()
    threading.Thread(target=bucle_medicion, daemon=True).start()
    threading.Thread(target=bucle_sesiones, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PUERTO), Handler)
    srv.daemon_threads = True
    log("escuchando en :%d · datos en %s · barrido WAHA cada %ds" % (PUERTO, DATA_DIR, BARRIDO_S))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())