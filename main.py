import requests
import time
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

# ======================================================
# 🔧 CARGA DE VARIABLES DE ENTORNO
# ======================================================
load_dotenv(override=False)

# ======================================================
# 🔥 CUENTAS MP + DESTINO (ROUTING)
# ======================================================
MP_ACCOUNTS = [
    {
        "nombre": "MP_YO",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN"),
        "DESTINO": os.getenv("WEBAPP_URL_SHEET_1")
    },
]

# ======================================================
# ❗ VALIDACIONES CRÍTICAS
# ======================================================
for acc in MP_ACCOUNTS:
    if not acc["ACCESS_TOKEN"]:
        raise RuntimeError(f"❌ Falta ACCESS_TOKEN para {acc['nombre']}")
    if not acc["DESTINO"]:
        raise RuntimeError(f"❌ Falta DESTINO (WEBAPP_URL) para {acc['nombre']}")

# ======================================================
# 🕒 FECHA DE ARRANQUE
# ======================================================
inicio_dt = datetime.now(timezone.utc)

def formato_mp(dt):
    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

inicio_script = formato_mp(inicio_dt)

print("🟢 Script iniciado")
print("🕒 Consultando operaciones DESDE:", inicio_script)

# ======================================================
# 📦 CACHE DE PROCESADOS (por cuenta)
# ======================================================
procesados = {acc["nombre"]: set() for acc in MP_ACCOUNTS}
MAX_IDS = 5000

# ======================================================
# 📌 CONSULTAR OPERACIONES MP
# ======================================================
def obtener_operaciones(access_token):
    url = "https://api.mercadopago.com/v1/payments/search"
    params = {
        "sort": "date_created",
        "criteria": "desc",
        "limit": 5,
        "begin_date": inicio_script
    }
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ MP error {resp.status_code}: {resp.text}")
            return []
        return resp.json().get("results", [])
    except Exception as e:
        print("❌ Error MP:", e)
        return []

# ======================================================
# 📌 CONVERTIR OPERACIÓN
# ======================================================
def convertir_op(op, origen):
    td = op.get("transaction_details", {}) or {}
    poi = op.get("point_of_interaction", {}) or {}
    tdata = poi.get("transaction_data", {}) or {}

    return {
        "id": op.get("id"),
        "origen": origen,
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
    print("🔁 Loop principal activo")

    while True:
        try:
            # 🔀 Lotes por destino
            lotes = {}

            for acc in MP_ACCOUNTS:
                nombre = acc["nombre"]
                token = acc["ACCESS_TOKEN"]
                destino = acc["DESTINO"]

                if destino not in lotes:
                    lotes[destino] = []

                ops = obtener_operaciones(token)

                for op in ops:
                    op_id = str(op.get("id"))

                    try:
                        fecha_op = datetime.fromisoformat(
                            op["date_created"].replace("Z", "+00:00")
                        )
                    except Exception:
                        continue

                    if fecha_op < inicio_dt:
                        continue
                    if op_id in procesados[nombre]:
                        continue

                    payer = op.get("payer", {}) or {}
                    dni = payer.get("identification", {}).get("number")
                    email = payer.get("email")

                    if (not dni) and (not email):
                        continue

                    lotes[destino].append(convertir_op(op, origen=nombre))
                    procesados[nombre].add(op_id)

                    if len(procesados[nombre]) > MAX_IDS:
                        procesados[nombre].clear()

            # 📤 Enviar cada lote a su Sheet
            for destino, lote in lotes.items():
                if not lote:
                    continue
                try:
                    r = requests.post(destino, json=lote, timeout=15)
                    print(f"📤 {len(lote)} ops → {destino} [{r.status_code}]")
                except Exception as e:
                    print("❌ Error enviando a GAS:", e)

        except Exception as e:
            print("🔥 ERROR GENERAL:", e)

        time.sleep(40)

# ======================================================
# 🚀 ENTRADA
# ======================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as fatal:
        print("💀 CRASH FATAL:", fatal)
        sys.exit(1)
