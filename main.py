import requests
import time
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

# ======================================================
# 🔧 CARGA DE VARIABLES DE ENTORNO
# ======================================================
load_dotenv(override=True)

# ======================================================
# 🔥 CUENTAS MP + DESTINO
# ======================================================
MP_ACCOUNTS = [
    {
        "nombre": "MP_AlbertoVera",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN"),
        "DESTINO": os.getenv("WEBAPP_URL_SHEET_1")
    },
    {
        "nombre": "MP_LeandroVera",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN2"),
        "DESTINO": os.getenv("WEBAPP_URL_SHEET_1")
    },
]

# ======================================================
# ❗ VALIDACIONES
# ======================================================
for acc in MP_ACCOUNTS:
    if not acc["ACCESS_TOKEN"]:
        raise RuntimeError(f"❌ Falta ACCESS_TOKEN para {acc['nombre']}")
    if not acc["DESTINO"]:
        raise RuntimeError(f"❌ Falta DESTINO para {acc['nombre']}")

# ======================================================
# 🕒 FECHA DE ARRANQUE
# ======================================================
inicio_dt = datetime.now(timezone.utc)

def formato_mp(dt):
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

print("🟢 Script iniciado")
print("🕒 Inicio UTC:", inicio_dt.isoformat())

# ======================================================
# 📦 CACHE DE IDS
# ======================================================
procesados = {acc["nombre"]: set() for acc in MP_ACCOUNTS}
MAX_IDS = 5000

# ======================================================
# 📡 CONSULTA MP
# ======================================================
def obtener_operaciones(access_token, nombre):
    url = "https://api.mercadopago.com/v1/payments/search"
    params = {
        "sort": "date_created",
        "criteria": "desc",
        "limit": 5
    }
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ {nombre} | MP {resp.status_code}: {resp.text}")
            return []
        results = resp.json().get("results", [])
        print(f"📥 {nombre} | MP devolvió {len(results)} ops")
        return results
    except Exception as e:
        print(f"❌ {nombre} | Error MP:", e)
        return []

# ======================================================
# 🔁 LOOP PRINCIPAL
# ======================================================
def main():
    print("🔁 Loop principal activo")

    while True:
        try:
            lotes = {}

            for acc in MP_ACCOUNTS:
                nombre = acc["nombre"]
                destino = acc["DESTINO"]

                stats = {
                    "total": 0,
                    "viejas": 0,
                    "repetidas": 0,
                    "sin_datos": 0,
                    "aceptadas": 0
                }

                ops = obtener_operaciones(acc["ACCESS_TOKEN"], nombre)

                if destino not in lotes:
                    lotes[destino] = []

                for op in ops:
                    stats["total"] += 1
                    op_id = str(op.get("id"))

                    try:
                        fecha_op = datetime.fromisoformat(
                            op["date_created"].replace("Z", "+00:00")
                        )
                    except Exception:
                        continue

                    if fecha_op < inicio_dt:
                        stats["viejas"] += 1
                        continue

                    if op_id in procesados[nombre]:
                        stats["repetidas"] += 1
                        continue

                    payer = op.get("payer") or {}
                    dni = payer.get("identification", {}).get("number")
                    email = payer.get("email")

                    if not dni and not email:
                        stats["sin_datos"] += 1
                        continue

                    lotes[destino].append(op)
                    procesados[nombre].add(op_id)
                    stats["aceptadas"] += 1

                print(
                    f"📊 {nombre} | total={stats['total']} "
                    f"viejas={stats['viejas']} "
                    f"rep={stats['repetidas']} "
                    f"sin_datos={stats['sin_datos']} "
                    f"aceptadas={stats['aceptadas']}"
                )

            # 📤 Envío
            for destino, lote in lotes.items():
                if not lote:
                    print(f"📭 Sin datos para enviar → {destino}")
                    continue
                try:
                    r = requests.post(destino, json=lote, timeout=15)
                    print(f"📤 Enviadas {len(lote)} ops → {destino} [{r.status_code}]")
                except Exception as e:
                    print("❌ Error enviando a GAS:", e)

        except Exception as e:
            print("🔥 ERROR GENERAL:", e)

        time.sleep(40)

# ======================================================
# 🚀 START
# ======================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as fatal:
        print("💀 CRASH FATAL:", fatal)
        sys.exit(1)
