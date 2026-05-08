import requests
import time
import os
import sys
import json
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
    {
        "nombre": "MP_DIEGO",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKENDIEG"),
        "DESTINO": [
            url.strip()
            for url in os.getenv("WEBAPP_URL_SHEET_2", "").split(",")
            if url.strip()
        ]
    },
    {
        "nombre": "MP_HECTOR",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKENHECTOR"),
        "DESTINO": [
            url.strip()
            for url in os.getenv("WEBAPP_URL_SHEET_WINSURF", "").split(",")
            if url.strip()
        ]
    },
    {
        "nombre": "MP_GUSTAVO",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKENGUS"),
        "DESTINO": [
            url.strip()
            for url in os.getenv("WEBAPP_URL_SHEET_WINSURF", "").split(",")
            if url.strip()
        ]
    },
    {
        "nombre": "MP_NOELIA",
        "ACCESS_TOKEN": os.getenv("MP_ACCESS_TOKENNOELIA"),
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
# 🕒 RELOJES PERSISTENTES (FIX DE REINICIOS)
# ======================================================
ARCHIVO_RELOJES = "relojes_estado.json"

def cargar_relojes():
    try:
        if os.path.exists(ARCHIVO_RELOJES):
            with open(ARCHIVO_RELOJES, "r") as f:
                datos = json.load(f)
                return {k: datetime.fromisoformat(v) for k, v in datos.items()}
    except Exception as e:
        print("⚠️ Error leyendo relojes guardados:", e)
    
    # Si no hay archivo, empieza desde el momento actual
    return {acc["nombre"]: datetime.now(timezone.utc) for acc in MP_ACCOUNTS}

def guardar_relojes(relojes):
    try:
        datos = {k: v.isoformat() for k, v in relojes.items()}
        with open(ARCHIVO_RELOJES, "w") as f:
            json.dump(datos, f)
    except Exception as e:
        print("⚠️ Error guardando relojes:", e)

relojes_cuentas = cargar_relojes()

def formato_mp(dt):
    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

print("🟢 Script iniciado")

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
# 📌 CONSULTAR OPERACIONES MP (PAGINADO + REINTENTOS)
# ======================================================
def obtener_operaciones(access_token, desde, max_reintentos=3):
    url = "https://api.mercadopago.com/v1/payments/search"
    hasta = datetime.now(timezone.utc)
    
    todas_las_operaciones = []
    offset = 0
    limit = 50 
    
    headers = {"Authorization": f"Bearer {access_token}"}

    while True:
        params = {
            "sort": "date_last_updated",
            "criteria": "desc",
            "limit": limit,
            "offset": offset,
            "range": "date_last_updated",
            "begin_date": formato_mp(desde),
            "end_date": formato_mp(hasta)
        }

        for intento in range(1, max_reintentos + 1):
            try:
                r = session.get(url, headers=headers, params=params, timeout=10)
                
                if r.status_code == 200:
                    resultados_pagina = r.json().get("results", [])
                    todas_las_operaciones.extend(resultados_pagina)
                    break 
                    
                if r.status_code in [500, 502, 503, 504]:
                    if intento < max_reintentos:
                        time.sleep(2 * intento)
                        continue
                    else:
                        print("❌ Se agotaron los reintentos con MP.")
                        return todas_las_operaciones
                else:
                    print(f"⚠️ MP {r.status_code}: {r.text}")
                    return todas_las_operaciones

            except (requests.Timeout, requests.ConnectionError):
                if intento < max_reintentos:
                    time.sleep(2 * intento)
                else:
                    return todas_las_operaciones
            except Exception as e:
                print("❌ Error inesperado MP:", repr(e))
                return todas_las_operaciones
        
        # Paginación: si devolvió la cantidad máxima, pedimos la siguiente página
        if 'resultados_pagina' in locals() and len(resultados_pagina) == limit:
            offset += limit 
        else:
            break 

    return todas_las_operaciones

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
    print("🔁 Loop activo")

    while True:
        try:
            lotes = {}

            for acc in MP_ACCOUNTS:
                nombre = acc["nombre"]
                token = acc["ACCESS_TOKEN"]
                destinos = acc["DESTINO"]
                
                if isinstance(destinos, str):
                    destinos = [destinos]
                
                for d in destinos:
                    lotes.setdefault(d, [])

                reloj_actual_cuenta = relojes_cuentas[nombre]
                desde_seguro = reloj_actual_cuenta - timedelta(minutes=5)

                ops = obtener_operaciones(token, desde_seguro)

                for op in ops:
                    op_id = str(op.get("id"))
                    if not op_id:
                        continue

                    try:
                        # 🔥 FIX: Usamos date_last_updated para actualizar el reloj
                        fecha_op = datetime.fromisoformat(
                            op["date_last_updated"].replace("Z", "+00:00")
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
                        lotes[d].append(lote_op.copy())
                        
                    procesados[nombre].append(op_id)

                    if fecha_op > relojes_cuentas[nombre]:
                        relojes_cuentas[nombre] = fecha_op

            # 💾 GUARDAR ESTADO DESPUÉS DE REVISAR LAS CUENTAS
            guardar_relojes(relojes_cuentas)

            # ==================================================
            # 📤 ENVÍO A DESTINOS (GAS / RAILWAY)
            # ==================================================
            for destino, lote in lotes.items():
                if not lote:
                    continue
                
                tipo_destino = "RAILWAY" if "railway.app" in destino else "SHEETS"

                try:
                    r = session.post(destino, json=lote, timeout=15)
                    if r.status_code == 200:
                        print(f"📤 {len(lote)} ops → {tipo_destino} (OK)")
                    else:
                        print(f"❌ ERROR {tipo_destino} ({r.status_code}): {r.text}")
                except requests.Timeout:
                    print(f"⏱ Timeout en envío a {tipo_destino}")
                except Exception as e:
                    print(f"❌ Error al enviar a {tipo_destino}:", repr(e))

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