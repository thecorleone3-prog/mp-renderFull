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

WEBAPP_URL = os.getenv("WEBAPP_URL")

# ======================================================
# 🔥 LISTA DE CUENTAS MP A CONSULTAR
# ======================================================
MP_ACCOUNTS = [
    {"nombre": "MP_MARISA",  "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN")},
    {"nombre": "MP_OSCAR",   "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN2")},
    {"nombre": "MP_VEIGA",   "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN4")},
    {"nombre": "MP_ZANONI",  "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN5")},
    {"nombre": "MP_MASULLO", "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKEN6")},
]

# ======================================================
# ❗ VALIDAR VARIABLES CRÍTICAS
# ======================================================
if not WEBAPP_URL:
    raise RuntimeError("❌ Falta WEBAPP_URL")

for acc in MP_ACCOUNTS:
    if not acc["ACCESS_TOKEN"]:
        raise RuntimeError(f"❌ Falta ACCESS_TOKEN para {acc['nombre']}")

# ======================================================
# 🕒 FECHA DE ARRANQUE GLOBAL
# ======================================================
inicio_dt = datetime.now(timezone.utc)

def formato_mp(dt):
    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

inicio_script = formato_mp(inicio_dt)

print("🟢 Script iniciado")
print("🕒 Consultando operaciones DESDE:", inicio_script)

# ======================================================
# 📦 CACHE DE OPERACIONES PROCESADAS (por cuenta)
# ======================================================
procesados = {acc["nombre"]: set() for acc in MP_ACCOUNTS}
MAX_IDS = 5000  # evita crecimiento infinito

# ======================================================
# 📌 CONSULTAR OPERACIONES DE UNA CUENTA MP
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
        resp = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=10
        )

        if resp.status_code != 200:
            print(f"⚠️ MP error {resp.status_code}: {resp.text}")
            return []

        return resp.json().get("results", [])

    except requests.RequestException as e:
        print("❌ Error consultando MP:", e)
        return []

# ======================================================
# 📌 CONVERTIR OPERACIÓN
# ======================================================
def convertir_op(op, origen):
    transaction_details = op.get("transaction_details", {}) or {}
    poi = op.get("point_of_interaction", {}) or {}
    transaction_data = poi.get("transaction_data", {}) or {}

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

        "bank_transfer_id": transaction_details.get("bank_transfer_id"),
        "acquirer_reference": transaction_details.get("acquirer_reference"),
        "e2e_id": transaction_data.get("e2e_id"),
        "transfer_account_id": (
            transaction_data
            .get("bank_info", {})
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
            lote_total = []

            for acc in MP_ACCOUNTS:
                nombre = acc["nombre"]
                token = acc["ACCESS_TOKEN"]

                ops = obtener_operaciones(token)

                for op in ops:
                    op_id = str(op.get("id"))

                    # 🛡️ Parseo seguro de fecha
                    try:
                        fecha_op = datetime.fromisoformat(
                            op["date_created"].replace("Z", "+00:00")
                        )
                    except Exception:
                        continue

                    # ❌ Anteriores al arranque
                    if fecha_op < inicio_dt:
                        continue

                    # ❌ Duplicados
                    if op_id in procesados[nombre]:
                        continue

                    # 🔥 FILTRO ANTI-SALIDAS
                    payer = op.get("payer", {}) or {}
                    dni = payer.get("identification", {}).get("number")
                    email = payer.get("email")

                    es_saliente = (not dni) and (not email)
                    if es_saliente:
                        continue

                    # ✔ Guardar operación válida
                    lote_total.append(convertir_op(op, origen=nombre))
                    procesados[nombre].add(op_id)

                    # 🧹 Limpiar cache si crece mucho
                    if len(procesados[nombre]) > MAX_IDS:
                        procesados[nombre].clear()

            # 📤 Enviar lote al GAS
            if lote_total:
                try:
                    r = requests.post(
                        WEBAPP_URL,
                        json=lote_total,
                        timeout=15
                    )
                    print(f"📤 Enviadas {len(lote_total)} ops → {r.status_code}")
                except requests.RequestException as e:
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
        sys.exit(1)  # Render reinicia el worker
