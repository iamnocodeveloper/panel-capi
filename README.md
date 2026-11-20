# Panel de flota Capi

Dashboard oscuro estilo Grafana para la flota Capi: servidores (CPU, memoria y disco),
sesiones de WhatsApp (WAHA) por servidor, sesiones caídas y estado de capi-ops.

- **Aplicación autocontenida**: solo biblioteca estándar de Python 3.12, sin dependencias.
- **Datos en vivo** desde dos fuentes por HTTP:
  - `GET /health` de capi-ops → servidores, carga, apps, incidencias, Telegram.
  - `GET /api/sessions?all=true` de cada instancia WAHA de la flota → inventario de sesiones.
  - *(Opcional)* `GET /metrics` de capi-ops (Prometheus).
- **Histórico rodante** en memoria (un punto cada 60 s, ~8 h) con volcado a disco para
  sobrevivir reinicios.
- **Histórico largo persistido**: agregados por hora (10 días) y por día (30 días) con
  promedio, máximo y mínimo por servidor y métrica.
- **Picos, patrones, tendencias y eventos**: los 5 picos más altos por servidor y los
  globales de la flota, promedio por hora del día (0-23) y por día de la semana, serie
  diaria de 7 y 30 días, y un log de alarmas (servidor inalcanzable, umbrales de 85 % y
  92 % de CPU/memoria/disco, cambios de incidencias abiertas y saltos de sesiones caídas).
- **HTTP Basic obligatorio** (`PANEL_USER` / `PANEL_PASS`): el panel muestra nombres de
  líneas de WhatsApp; sin credenciales no sirve nada.

## Rutas

| Ruta | Descripción |
| --- | --- |
| `GET /` | Dashboard (`index.html`). |
| `GET /api/data` | JSON agregado: KPIs, servidores, histórico corto (`historico`), serie horaria (`serie_horaria`), `picos`, `por_hora`, `por_semana`, `tendencia`, `eventos`, `cobertura` y sesiones. |
| `GET /api/eventos` | Log de eventos persistido. Parámetros opcionales: `limite` (por defecto 300), `alias`, `tipo`. |
| `GET /api/health` | `{"ok":…, "edad_sesiones_seg":…, "fuente_capi_ops":…, "cubos_horarios":…, "eventos":…}`. |
| `GET /api/refresh` | Fuerza una medición ahora (y un barrido de sesiones si el inventario está viejo). |

Todas requieren HTTP Basic.

## Archivos persistidos en `DATA_DIR`

| Archivo | Contenido |
| --- | --- |
| `historico.json` | Puntos de 60 s (últimas ~8 h). |
| `agregados.json` | Cubos horarios (10 días), cubos diarios (30 días), picos y estado de umbrales. |
| `eventos.json` | Log de eventos y alarmas (hasta 3000 entradas). |
| `sesiones.json` | Último inventario de sesiones WAHA. |

## Variables de entorno

| Variable | Por defecto | Descripción |
| --- | --- | --- |
| `PANEL_USER` | — | **Obligatoria**. Usuario de HTTP Basic. |
| `PANEL_PASS` | — | **Obligatoria**. Contraseña de HTTP Basic. |
| `WAHA_API_KEY` | — | Clave de la API de WAHA (cabecera `X-Api-Key`). Sin ella no hay inventario de sesiones. |
| `OPS_HEALTH_URL` | `http://207.231.110.162:18080/health` | Endpoint de health de capi-ops. |
| `OPS_METRICS_URL` | `http://207.231.110.162:18080/metrics` | Endpoint Prometheus (opcional). |
| `DATA_DIR` | `/data` (cae a `./data`) | Directorio del histórico y del último inventario. |
| `PORT` | `8080` | Puerto de escucha. |

## Ejecutar en local

```bash
export PANEL_USER=admin PANEL_PASS=... WAHA_API_KEY=...
export DATA_DIR=./data PORT=8080
python server.py
```

## Docker

```bash
docker build -t panel-capi .
docker run --rm -p 8080:8080 -v panel-capi-data:/data \
  -e PANEL_USER=admin -e PANEL_PASS=... -e WAHA_API_KEY=... panel-capi
```

Las credenciales **nunca** se guardan en el repositorio: se pasan por variables de entorno
del servicio (EasyPanel → *Environment*).

## Rangos de la flota

capi1 = waha2..17 · capi2 = waha18..28 · capi3 = waha29..45 · capi4 = waha46..60 ·
capi5 = waha61..74 · capi6 = waha75..84 · capi7 = waha85..94 · capi8 = waha95..104
(+ `blackbet`) · capi9 = waha105..114 · `wahacapi` = servidor WAHA sin panel.
