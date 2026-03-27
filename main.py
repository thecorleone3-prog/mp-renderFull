import requests
import time
import os
import sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from collections import deque

# ======================================================
# 🔧 CARGA DE VARIABLES DE ENTORNO
# ======================================================
load_dotenv(override=True)

# ======================================================
# 🔥 CUENTAS MP + DESTINO
# ======================================================
MP_ACCOUNTS = [
    # =========================
    # 📄 SHEET CROWN
    # =========================
    {
        "nombre": "MP_DIEGO",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKENDIEG"),
        "DESTINO": [
            url.strip()
            for url in os.getenv("WEBAPP_URL_SHEET_2", "").split(",")
            if url.strip()
        ]
    },
    # =========================
    # 📄 SHEET WinSurf
    # =========================
    {
        "nombre": "MP_HECTOR",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKENHECTOR"),
        "DESTINO": [
            url.strip()
            for url in os.getenv("WEBAPP_URL_SHEET_WINSURF", "").split(",")
            if url.strip()
        ]
    }
]

# ======================================================
# ❗ VALIDACIONES
# ======================================================
for acc in MP_ACCOUNTS:
    if not acc["ACCESS_TOKEN"]:
        raise RuntimeError(f"❌ Falta ACCESS_TOKEN para {acc['nombre']}")
    if not acc["DESTINO"] or (isinstance(acc["DESTINO"], list) and len(acc["DESTINO"]) == 0):
        raise RuntimeError(f"❌ Falta DESTINO para {acc['nombre']}")

# ======================================================
# 🕒 FECHA INICIAL (UTC)
# ======================================================
inicio_dt = datetime.now(timezone.utc)
ultimo_dt = inicio_dt

def formato_mp(dt):
    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

print("🟢 Script iniciado")
print("🕒 Inicio:", formato_mp(inicio_dt))

# ======================================================
# 📦 CACHE FIFO DE IDS PROCESADOS
# ======================================================
MAX_IDS = 5000
procesados = {
    acc["nombre"]: deque(maxlen=MAX_IDS)
    for acc in MP_ACCOUNTS
}

# ======================================================
# 🌐 SESSION HTTP
# ======================================================
session = requests.Session()

# ======================================================
# 📌 CONSULTAR OPERACIONES MP
# ======================================================
def obtener_operaciones(access_token, desde):
    url = "https://api.mercadopago.com/v1/payments/search"
    params = {
        "sort": "date_created",
        "criteria": "desc",
        "limit": 20,
        "begin_date": formato_mp(desde)
    }
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        r = session.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ MP {r.status_code}: {r.text}")
            return []
        return r.json().get("results", [])
    except requests.Timeout:
        print("⏱ Timeout MP")
    except requests.ConnectionError:
        print("🌐 Error conexión MP")
    except Exception as e:
        print("❌ Error MP:", repr(e))

    return []

# ======================================================
# 📌 NORMALIZAR OPERACIÓN
# ======================================================
def convertir_op(op, origen, direccion):
    td = op.get("transaction_details") or {}
    poi = op.get("point_of_interaction") or {}
    tdata = poi.get("transaction_data") or {}

    return {
        "id": op.get("id"),
        "origen": origen,
        "direccion": direccion,

        "monto": op.get("transaction_amount"),
        "fecha": op.get("date_created"),
        "estado": op.get("status"),
        "tipo": op.get("operation_type"),

        "dni": op.get("payer", {}).get("identification", {}).get("number"),
        "email": op.get("payer", {}).get("email"),
        "nombre": op.get("payer", {}).get("first_name"),
        "apellido": op.get("payer", {}).get("last_name"),

        "bank_transfer_id": td.get("bank_transfer_id"),
        "acquirer_reference": td.get("acquirer_reference"),
        "e2e_id": tdata.get("e2e_id"),
        "transfer_account_id": (
            tdata.get("bank_info", {})
            .get("collector", {})
            .get("transfer_account_id")
        )
    }

# ======================================================
# 🔁 LOOP PRINCIPAL
# ======================================================
def main():
    global ultimo_dt
    print("🔁 Loop activo")

    while True:
        try:
            lotes = {}

            for acc in MP_ACCOUNTS:
                nombre = acc["nombre"]
                token = acc["ACCESS_TOKEN"]
                destinos = acc["DESTINO"]
                # asegurar lista (por si alguna quedó como string)
                if isinstance(destinos, str):
                    destinos = [destinos]
                
                for d in destinos:
                    lotes.setdefault(d, [])

                # ⬅️ BUFFER DE SEGURIDAD (5 MIN)
                desde_seguro = ultimo_dt - timedelta(minutes=5)

                ops = obtener_operaciones(token, desde_seguro)

                for op in ops:
                    op_id = str(op.get("id"))
                    if not op_id:
                        continue

                    try:
                        fecha_op = datetime.fromisoformat(
                            op["date_created"].replace("Z", "+00:00")
                        )
                    except Exception:
                        continue

                    if op_id in procesados[nombre]:
                        continue

                    # ============================
                    # 🔥 CLASIFICACIÓN DIRECCIÓN
                    # ============================
                    payer = op.get("payer", {}) or {}
                    dni = payer.get("identification", {}).get("number")
                    email = payer.get("email")

                    direccion = "SALIENTE" if (not dni) and (not email) else "ENTRANTE"

                    lote_op = convertir_op(op, nombre, direccion)
                    for d in destinos:
                        lotes[d].append(lote_op)
                    procesados[nombre].append(op_id)

                    # ⬅️ SOLO AVANZA, NUNCA RETROCEDE
                    if fecha_op > ultimo_dt:
                        ultimo_dt = fecha_op

            # ==================================================
            # 📤 ENVÍO A GOOGLE APPS SCRIPT
            # ==================================================
            for destino, lote in lotes.items():
                if not lote:
                    continue

                try:
                    r = session.post(destino, json=lote, timeout=15)
                    if r.status_code == 200:
                        print(f"📤 {len(lote)} ops → OK")
                    else:
                        print(f"❌ GAS {r.status_code}: {r.text}")
                except requests.Timeout:
                    print("⏱ Timeout GAS")
                except Exception as e:
                    print("❌ Error GAS:", repr(e))

        except Exception as e:
            print("🔥 ERROR GENERAL:", repr(e))

        time.sleep(40)

# ======================================================
# 🚀 ENTRADA
# ======================================================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("⏹ Detenido manualmente")
        sys.exit(0)
    except Exception as fatal:
        print("💀 CRASH FATAL:", repr(fatal))
        sys.exit(1)
