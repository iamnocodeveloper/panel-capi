#!/usr/bin/env python3
"""Panel de métricas de la flota Capi.

Servidor HTTP autocontenido (solo stdlib) que:
  - sirve el dashboard (index.html),
  - mide capi-ops (/health) cada 60 s y guarda un histórico rodante en memoria + disco,
  - agrega ese histórico en cubos horarios (10 días) y diarios (30 días) persistidos,
  - registra picos por servidor y de la flota, y un log de eventos/alarmas,
  - barre el inventario de sesiones WAHA de la flota cada 20 min en segundo plano,
  - expone GET /api/data, GET /api/eventos y GET /api/health con HTTP Basic.

No escribe ni registra credenciales en ningún log.
"""
import base64
import datetime as dt
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
MAX_PUNTOS = 480          # ~8 h de histórico rodante en memoria (detalle fino)
BARRIDO_S = 1200          # cada 20 min el inventario de sesiones
HILOS_WAHA = 20
TIMEOUT_WAHA = 15
TIMEOUT_OPS = 12

RET_HORAS = 10            # días de detalle horario
RET_DIAS = 30             # días de agregados diarios
MAX_PICOS_SRV = 5         # picos guardados por servidor
MAX_PICOS_GLOBAL = 8      # picos guardados de la flota
MAX_EVENTOS = 3000        # eventos persistidos
UMBRALES = (85.0, 92.0)   # umbrales de CPU/Memoria/Disco

TZ_EC = dt.timezone(dt.timedelta(hours=-5))   # Ecuador, sin horario de verano
DIAS_SEM = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")
METRICAS_SRV = ("cpu", "mem", "disco")

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
F_AGR = os.path.join(DATA_DIR, "agregados.json")
F_EVT = os.path.join(DATA_DIR, "eventos.json")

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
    # --- histórico largo (persistido)
    "agregados": {"horas": {}, "dias": {}},
    "picos": {"por_servidor": {}, "global": []},
    "eventos": [],
    "estado_previo": {},    # alias -> {reachable, cpu_nivel, mem_nivel, disco_nivel, failed}
    "incidencias_prev": None,
    "eventos_sucios": False,
    "agregados_sucios": False,
}


def log(msg):
    print("[panel-capi] %s %s" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg), flush=True)


def ahora_iso(t=None):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t if t is not None else time.time()))


def iso_ec(t):
    return dt.datetime.fromtimestamp(float(t), TZ_EC).strftime("%Y-%m-%d %H:%M:%S")


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


def _num(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:      # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def hora_cubo(t):
    return int(float(t) // 3600 * 3600)


def clave_dia(t):
    return dt.datetime.fromtimestamp(float(t), TZ_EC).strftime("%Y-%m-%d")


def nivel_umbral(v):
    if v is None:
        return 0
    if v >= UMBRALES[1]:
        return 2
    if v >= UMBRALES[0]:
        return 1
    return 0


# --------------------------------------------------------------------------- histórico corto
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


# --------------------------------------------------------------------------- histórico largo
def cargar_agregados():
    nuevo = True
    try:
        if os.path.exists(F_AGR):
            d = json.load(open(F_AGR, encoding="utf-8"))
            with LOCK:
                ESTADO["agregados"]["horas"] = d.get("horas") or {}
                ESTADO["agregados"]["dias"] = d.get("dias") or {}
                ESTADO["picos"]["por_servidor"] = d.get("picos_servidor") or {}
                ESTADO["picos"]["global"] = d.get("picos_global") or []
                ESTADO["estado_previo"] = d.get("estado_previo") or {}
                ESTADO["incidencias_prev"] = d.get("incidencias_prev")
            nuevo = False
            n_h = sum(len(v) for v in (d.get("horas") or {}).values())
            n_d = sum(len(v) for v in (d.get("dias") or {}).values())
            log("agregados cargados: %d cubos horarios, %d cubos diarios" % (n_h, n_d))
    except Exception as e:
        log("no se pudieron cargar los agregados: %s" % e)

    backfill = False
    if nuevo:
        backfill = True
    else:
        with LOCK:
            backfill = not ESTADO["agregados"]["horas"]
    if backfill:
        # primera ejecución tras la ampliación: sembrar los cubos con el histórico rodante
        with LOCK:
            pts = list(ESTADO["puntos"])
        n = 0
        for p in pts:
            acumular(p)
            n += 1
        if n:
            log("cubos sembrados desde el histórico rodante: %d puntos" % n)

    try:
        if os.path.exists(F_EVT):
            d = json.load(open(F_EVT, encoding="utf-8"))
            with LOCK:
                ESTADO["eventos"] = (d.get("eventos") or [])[-MAX_EVENTOS:]
            log("eventos cargados: %d" % len(ESTADO["eventos"]))
    except Exception as e:
        log("no se pudieron cargar los eventos: %s" % e)


def acumular(punto):
    """Suma un punto a los cubos horario y diario (y actualiza picos)."""
    t = punto.get("t")
    if not t:
        return
    hk = str(hora_cubo(t))
    dk = clave_dia(t)
    with LOCK:
        horas = ESTADO["agregados"]["horas"]
        dias = ESTADO["agregados"]["dias"]
        for alias, arr in (punto.get("s") or {}).items():
            if not arr:
                continue
            h_srv = horas.setdefault(alias, {})
            d_srv = dias.setdefault(alias, {})
            for mi, met in enumerate(METRICAS_SRV):
                v = _num(arr[mi]) if mi < len(arr) else None
                if v is None:
                    continue
                hb = h_srv.setdefault(hk, {}).setdefault(met, {"n": 0, "s": 0.0, "max": v, "min": v})
                hb["n"] += 1
                hb["s"] = round(hb["s"] + v, 4)
                hb["max"] = max(hb["max"], v)
                hb["min"] = min(hb["min"], v)
                db = d_srv.setdefault(dk, {}).setdefault(met, {"n": 0, "s": 0.0, "max": v, "min": v})
                db["n"] += 1
                db["s"] = round(db["s"] + v, 4)
                db["max"] = max(db["max"], v)
                db["min"] = min(db["min"], v)
                registrar_pico(alias, met, v, t)
        ESTADO["agregados_sucios"] = True


def registrar_pico(alias, met, v, t):
    """Guarda los máximos por servidor y de la flota, sin repetir el mismo pico cada minuto."""
    def insertar(lst, tope):
        for e in lst:
            if e["metrica"] == met and e["alias"] == alias and abs(e["t"] - t) <= 3600 \
                    and abs(e["valor"] - v) <= 1.0:
                if v > e["valor"]:
                    e["valor"] = round(v, 1)
                    e["t"] = int(t)
                return
        lst.append({"alias": alias, "metrica": met, "valor": round(v, 1), "t": int(t)})
        lst.sort(key=lambda x: -x["valor"])
        del lst[tope:]

    insertar(ESTADO["picos"]["por_servidor"].setdefault(alias, []), MAX_PICOS_SRV)
    insertar(ESTADO["picos"]["global"], MAX_PICOS_GLOBAL)


def podar_agregados():
    """Aplica la retención: horas RET_HORAS días, días RET_DIAS días."""
    ahora = time.time()
    corte_h = hora_cubo(ahora - RET_HORAS * 86400)
    corte_d = clave_dia(ahora - RET_DIAS * 86400)
    with LOCK:
        for alias, cubos in ESTADO["agregados"]["horas"].items():
            for k in [k for k in cubos if int(k) < corte_h]:
                del cubos[k]
        for alias, cubos in ESTADO["agregados"]["dias"].items():
            for k in [k for k in cubos if k < corte_d]:
                del cubos[k]


def persistir_agregados():
    with LOCK:
        if not ESTADO["agregados_sucios"]:
            return
        ESTADO["agregados_sucios"] = False
        payload = {
            "guardadoEn": ahora_iso(),
            "horas": ESTADO["agregados"]["horas"],
            "dias": ESTADO["agregados"]["dias"],
            "picos_servidor": ESTADO["picos"]["por_servidor"],
            "picos_global": ESTADO["picos"]["global"],
            "estado_previo": ESTADO["estado_previo"],
            "incidencias_prev": ESTADO["incidencias_prev"],
        }
    try:
        guardar_json_atomico(F_AGR, payload)
    except Exception as e:
        log("no se pudieron guardar los agregados: %s" % e)


# --------------------------------------------------------------------------- eventos
def anotar_evento(t, tipo, alias, valor=None, detalle="", nivel=None, metrica=None):
    ev = {
        "t": int(t),
        "iso": ahora_iso(t),
        "hora_ec": iso_ec(t),
        "tipo": tipo,
        "alias": alias,
        "metrica": metrica,
        "nivel": nivel,
        "valor": None if valor is None else round(float(valor), 1),
        "detalle": detalle,
    }
    with LOCK:
        lst = ESTADO["eventos"]
        lst.append(ev)
        if len(lst) > MAX_EVENTOS:
            del lst[: len(lst) - MAX_EVENTOS]
        ESTADO["eventos_sucios"] = True
    log("evento %s %s %s" % (tipo, alias, detalle))
    return ev


def persistir_eventos():
    with LOCK:
        if not ESTADO["eventos_sucios"]:
            return
        ESTADO["eventos_sucios"] = False
        payload = {"guardadoEn": ahora_iso(), "eventos": list(ESTADO["eventos"])}
    try:
        guardar_json_atomico(F_EVT, payload)
    except Exception as e:
        log("no se pudieron guardar los eventos: %s" % e)


NOMBRE_UMBRAL = {"cpu": "umbral_cpu", "mem": "umbral_mem", "disco": "umbral_disco"}


def evaluar_eventos(t, salud):
    """Compara el estado actual con el anterior y anota los cambios en el log de eventos."""
    previos = ESTADO["estado_previo"]
    vistos = set()
    for s in (salud or {}).get("servers") or []:
        a = s.get("alias")
        if not a:
            continue
        vistos.add(a)
        prev = previos.get(a) or {}
        reach = s.get("reachable")
        if reach is False and prev.get("reachable") is not False:
            anotar_evento(t, "servidor_inalcanzable", a, detalle="capi-ops no alcanza el servidor")
        elif reach is True and prev.get("reachable") is False:
            anotar_evento(t, "servidor_recuperado", a, detalle="el servidor vuelve a responder")
        nuevo = {"reachable": reach}
        for met in METRICAS_SRV:
            v = _num(s.get(met))
            niv = nivel_umbral(v)
            viejo = prev.get("%s_nivel" % met, 0)
            if niv > viejo and v is not None:
                anotar_evento(t, NOMBRE_UMBRAL[met], a, valor=v, metrica=met,
                              nivel=int(UMBRALES[niv - 1]),
                              detalle="%s en %.1f %% (umbral %d %%)" % (met, v, int(UMBRALES[niv - 1])))
            elif niv < viejo and v is not None:
                anotar_evento(t, "umbral_normalizado", a, valor=v, metrica=met,
                              nivel=0, detalle="%s bajó a %.1f %%" % (met, v))
            nuevo["%s_nivel" % met] = niv
        # conserva el conteo de caídas que mantiene el barrido WAHA
        nuevo["failed"] = prev.get("failed", 0)
        previos[a] = nuevo

    for a in [a for a in previos if a not in vistos]:
        if previos[a].get("reachable") is not False:
            previos[a]["reachable"] = False
            anotar_evento(t, "servidor_sin_datos", a, detalle="el servidor desapareció de capi-ops")

    inc = (salud or {}).get("openIncidents")
    prev_inc = ESTADO.get("incidencias_prev")
    if inc is not None and prev_inc != inc:
        if prev_inc is not None:
            d = inc - prev_inc
            anotar_evento(t, "incidencias_cambio", "flota", valor=inc,
                          detalle="incidencias abiertas %+d (%d)" % (d, inc))
        ESTADO["incidencias_prev"] = inc


def evaluar_eventos_sesiones(t):
    """Detecta saltos en el número de sesiones caídas por servidor."""
    with LOCK:
        hosts = [dict(h) for h in ESTADO["sesiones"]["hosts"]]
        previos = ESTADO["estado_previo"]
    fallos = {}
    for h in hosts:
        if not h.get("ok"):
            continue
        a = h.get("alias")
        n = sum(1 for s in h.get("sesiones") or [] if s.get("status") == "FAILED")
        fallos[a] = fallos.get(a, 0) + n
    for a, cur in fallos.items():
        p = previos.setdefault(a, {})
        if "failed" not in p:          # primera observación: solo se toma la línea base
            p["failed"] = cur
            continue
        prev = p["failed"]
        d = cur - prev
        if d != 0 and (abs(d) >= 2 or prev == 0 or cur == 0):
            anotar_evento(t, "sesiones_caidas", a, valor=cur,
                          detalle="sesiones caídas %+d (ahora %d)" % (d, cur))
        p["failed"] = cur


# --------------------------------------------------------------------------- medición
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
        punto["s"][a] = [_num(s.get("cpu")), _num(s.get("mem")), _num(s.get("disk"))]

    nuevo = False
    with LOCK:
        ESTADO["salud"] = salud
        ESTADO["salud_error"] = None
        ESTADO["medido_en"] = int(t)
        pts = ESTADO["puntos"]
        if pts and t - pts[-1]["t"] < PASO * 0.5:
            pts[-1] = punto
        else:
            pts.append(punto)
            nuevo = True
        if len(pts) > MAX_PUNTOS:
            del pts[: len(pts) - MAX_PUNTOS]
        copia = list(pts)

    if nuevo:
        acumular(punto)
    evaluar_eventos(t, salud)

    try:
        guardar_json_atomico(F_HIST, {"guardadoEn": ahora_iso(), "paso": PASO, "puntos": copia})
    except Exception as e:
        log("no se pudo guardar el histórico: %s" % e)
    podar_agregados()
    persistir_agregados()
    persistir_eventos()
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
        evaluar_eventos_sesiones(t0)
        persistir_eventos()
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


# --------------------------------------------------------------------------- agregados públicos
def _promedio(cubo):
    n = cubo.get("n") or 0
    return (cubo.get("s", 0.0) / n) if n else None


def construir_patrones():
    """Promedios por hora del día (0-23) y por día de la semana, de toda la flota y por servidor."""
    with LOCK:
        horas = {a: {k: {m: dict(v) for m, v in cubos.items()} for k, cubos in d.items()}
                 for a, d in ESTADO["agregados"]["horas"].items()}

    def acum(acc, met, v, n=1, nmax=None, nmin=None):
        d = acc.setdefault(met, {"s": 0.0, "n": 0, "max": None, "min": None})
        d["s"] += v * n
        d["n"] += n
        if nmax is not None:
            d["max"] = nmax if d["max"] is None else max(d["max"], nmax)
        if nmin is not None:
            d["min"] = nmin if d["min"] is None else min(d["min"], nmin)

    def cerrar(acc):
        out = {}
        for m in METRICAS_SRV:
            d = acc.get(m) or {"s": 0.0, "n": 0, "max": None, "min": None}
            out[m] = {"promedio": round(d["s"] / d["n"], 1) if d["n"] else None,
                      "max": None if d["max"] is None else round(d["max"], 1),
                      "min": None if d["min"] is None else round(d["min"], 1),
                      "muestras": d["n"]}
        return out

    flota_h = [{} for _ in range(24)]
    flota_w = [{} for _ in range(7)]
    srv_h = {}
    srv_w = {}
    for alias, cubos in horas.items():
        eh = [{} for _ in range(24)]
        ew = [{} for _ in range(7)]
        for hk, mets in cubos.items():
            local = dt.datetime.fromtimestamp(int(hk), TZ_EC)
            for met in METRICAS_SRV:
                cubo = mets.get(met)
                if not cubo or not cubo.get("n"):
                    continue
                prom = cubo["s"] / cubo["n"]
                for destinos, idx in ((eh, local.hour), (ew, local.weekday()),
                                      (flota_h, local.hour), (flota_w, local.weekday())):
                    acum(destinos[idx], met, prom, cubo["n"], cubo.get("max"), cubo.get("min"))
        srv_h[alias] = [cerrar(x) for x in eh]
        srv_w[alias] = [cerrar(x) for x in ew]

    horas_cubiertas = 0
    if horas:
        todas = set()
        for cubos in horas.values():
            todas |= set(cubos.keys())
        horas_cubiertas = len(todas)

    return {
        "por_hora": [cerrar(x) for x in flota_h],
        "por_semana": [cerrar(x) for x in flota_w],
        "dias_semana": list(DIAS_SEM),
        "por_hora_servidor": srv_h,
        "por_semana_servidor": srv_w,
        "horas_cubiertas": horas_cubiertas,
        "desde": min((k for cubos in horas.values() for k in cubos), default=None),
        "hasta": max((k for cubos in horas.values() for k in cubos), default=None),
    }


def construir_tendencias():
    """Serie diaria (promedio y máximo) de los últimos 30 días por servidor."""
    hoy = dt.datetime.now(TZ_EC).date()
    with LOCK:
        dias = {a: {k: {m: dict(v) for m, v in cubos.items()} for k, cubos in d.items()}
                for a, d in ESTADO["agregados"]["dias"].items()}
    salida = {}
    for alias, cubos in dias.items():
        serie = []
        for i in range(RET_DIAS - 1, -1, -1):
            fecha = (hoy - dt.timedelta(days=i)).strftime("%Y-%m-%d")
            item = {"fecha": fecha}
            cubo_dia = cubos.get(fecha) or {}
            for met in METRICAS_SRV:
                c = cubo_dia.get(met)
                if c and c.get("n"):
                    item[met + "_prom"] = round(c["s"] / c["n"], 1)
                    item[met + "_max"] = round(c["max"], 1)
                    item[met + "_min"] = round(c["min"], 1)
                else:
                    item[met + "_prom"] = None
                    item[met + "_max"] = None
                    item[met + "_min"] = None
            serie.append(item)
        salida[alias] = serie
    return salida


def construir_serie_horaria():
    """Serie horaria (promedio y máximo por hora) de los últimos RET_HORAS días, por servidor."""
    with LOCK:
        horas = ESTADO["agregados"]["horas"]
        salida = {}
        for a, cubos in horas.items():
            serie = []
            for hk in sorted(cubos, key=lambda k: int(k)):
                item = {"t": int(hk)}
                for met in METRICAS_SRV:
                    c = cubos[hk].get(met) or {}
                    n = c.get("n") or 0
                    item[met] = round(c["s"] / n, 1) if n else None
                    item[met + "_max"] = round(c["max"], 1) if n else None
                serie.append(item)
            salida[a] = serie
    return salida


def construir_picos():
    with LOCK:
        por_srv = {a: [dict(x, hora_ec=iso_ec(x["t"]), iso=ahora_iso(x["t"]))
                       for x in lst] for a, lst in ESTADO["picos"]["por_servidor"].items()}
        glob = [dict(x, hora_ec=iso_ec(x["t"]), iso=ahora_iso(x["t"])) for x in ESTADO["picos"]["global"]]
    glob.sort(key=lambda x: -x["valor"])
    return {"por_servidor": por_srv, "global": glob}


def construir_eventos(limite=200, alias=None, tipo=None):
    with LOCK:
        lst = list(ESTADO["eventos"])
    if alias:
        lst = [e for e in lst if e.get("alias") == alias]
    if tipo:
        lst = [e for e in lst if e.get("tipo") == tipo]
    lst.reverse()
    return lst[:limite] if limite else lst


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

    patrones = construir_patrones()
    tendencia = construir_tendencias()
    picos = construir_picos()
    eventos = construir_eventos(120)
    total_eventos = len(ESTADO["eventos"])

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
        "serie_horaria": construir_serie_horaria(),
        # --- ampliación: histórico largo, picos, patrones, eventos, tendencia
        "picos": picos,
        "por_hora": patrones["por_hora"],
        "por_semana": patrones["por_semana"],
        "dias_semana": patrones["dias_semana"],
        "por_hora_servidor": patrones["por_hora_servidor"],
        "por_semana_servidor": patrones["por_semana_servidor"],
        "cobertura": {
            "horas_cubiertas": patrones["horas_cubiertas"],
            "desde": patrones["desde"],
            "hasta": patrones["hasta"],
            "retencion_horaria_dias": RET_HORAS,
            "retencion_diaria_dias": RET_DIAS,
            "eventos": total_eventos,
        },
        "tendencia": tendencia,
        "eventos": eventos,
        "eventos_total": total_eventos,
    }


# --------------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "panel-capi/1.1"
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
            try:
                datos = construir_datos()
            except Exception as e:
                self._json(500, {"error": "%s: %s" % (type(e).__name__, e)})
                return
            self._json(200, datos)
            return
        if ruta == "/api/eventos":
            q = {}
            if "?" in self.path:
                for par in self.path.split("?", 1)[1].split("&"):
                    if "=" in par:
                        k, v = par.split("=", 1)
                        q[k] = urllib.request.unquote(v)
            try:
                limite = int(q.get("limite", "300"))
            except ValueError:
                limite = 300
            limite = max(0, min(limite, MAX_EVENTOS))
            eventos = construir_eventos(limite, q.get("alias"), q.get("tipo"))
            with LOCK:
                total = len(ESTADO["eventos"])
            self._json(200, {"generadoEn": ahora_iso(), "total": total,
                             "devueltos": len(eventos), "eventos": eventos})
            return
        if ruta == "/api/health":
            with LOCK:
                ses_t = ESTADO["sesiones"]["actualizadoEn"]
                salud = ESTADO["salud"]
                barriendo = ESTADO["sesiones"]["barriendo"]
                error = ESTADO["sesiones"]["error"]
                n_ev = len(ESTADO["eventos"])
                n_h = sum(len(v) for v in ESTADO["agregados"]["horas"].values())
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
                "cubos_horarios": n_h,
                "eventos": n_ev,
                "retencion_horaria_dias": RET_HORAS,
                "retencion_diaria_dias": RET_DIAS,
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
    cargar_agregados()
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