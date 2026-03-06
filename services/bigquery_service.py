import os
import json
import uuid
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "taller_demo")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

def _get_credentials():
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("Falta GOOGLE_CREDENTIALS_JSON")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    return service_account.Credentials.from_service_account_info(info)

def get_bq_client():
    credentials = _get_credentials()
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def _table(table_name: str) -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def log_conversation(telegram_id: str, role: str, mensaje: str):
    client = get_bq_client()
    rows = [{
        "event_id": str(uuid.uuid4()),
        "telegram_id": telegram_id,
        "role": role,
        "mensaje": mensaje,
        "timestamp": utc_now(),
    }]
    errors = client.insert_rows_json(_table("conversaciones"), rows)
    if errors:
        raise RuntimeError(f"Error insertando conversación: {errors}")

def get_cliente_by_telegram_id(telegram_id: str):
    client = get_bq_client()
    query = f"""
        SELECT *
        FROM `{_table("clientes")}`
        WHERE telegram_id = @telegram_id
        LIMIT 1
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("telegram_id", "STRING", telegram_id)
            ]
        )
    )
    rows = list(job.result())
    return dict(rows[0]) if rows else None

def create_cliente(telegram_id: str, nombre: str, telefono: str = None):
    client = get_bq_client()
    cliente_id = f"CLI-{uuid.uuid4().hex[:10]}"
    rows = [{
        "cliente_id": cliente_id,
        "telegram_id": telegram_id,
        "nombre": nombre,
        "telefono": telefono,
        "created_at": utc_now(),
    }]
    errors = client.insert_rows_json(_table("clientes"), rows)
    if errors:
        raise RuntimeError(f"Error insertando cliente: {errors}")
    return {"cliente_id": cliente_id, "telegram_id": telegram_id, "nombre": nombre}

def create_vehiculo(cliente_id: str, marca: str, modelo: str, anio: int, placa: str = None, km_actual: int = None):
    client = get_bq_client()
    vehiculo_id = f"VEH-{uuid.uuid4().hex[:10]}"
    rows = [{
        "vehiculo_id": vehiculo_id,
        "cliente_id": cliente_id,
        "marca": marca,
        "modelo": modelo,
        "anio": anio,
        "placa": placa,
        "km_actual": km_actual,
        "created_at": utc_now(),
    }]
    errors = client.insert_rows_json(_table("vehiculos"), rows)
    if errors:
        raise RuntimeError(f"Error insertando vehículo: {errors}")
    return {
        "vehiculo_id": vehiculo_id,
        "cliente_id": cliente_id,
        "marca": marca,
        "modelo": modelo,
        "anio": anio,
    }

def create_cita(cliente_id: str, vehiculo_id: str, fecha_iso: str, motivo: str, categoria: str,
                estimado_min: float, estimado_max: float, calendar_event_id: str, estado: str = "CITA_AGENDADA"):
    client = get_bq_client()
    cita_id = f"CIT-{uuid.uuid4().hex[:10]}"
    rows = [{
        "cita_id": cita_id,
        "cliente_id": cliente_id,
        "vehiculo_id": vehiculo_id,
        "fecha": fecha_iso,
        "motivo": motivo,
        "categoria": categoria,
        "estimado_min": estimado_min,
        "estimado_max": estimado_max,
        "calendar_event_id": calendar_event_id,
        "estado": estado,
        "created_at": utc_now(),
    }]
    errors = client.insert_rows_json(_table("citas"), rows)
    if errors:
        raise RuntimeError(f"Error insertando cita: {errors}")
    return {"cita_id": cita_id}

def get_upcoming_citas(days_ahead: int = 3):
    client = get_bq_client()
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=days_ahead)

    query = f"""
        SELECT
          c.cita_id,
          c.fecha,
          c.motivo,
          cl.telegram_id,
          cl.nombre,
          v.marca,
          v.modelo,
          v.anio
        FROM `{_table("citas")}` c
        JOIN `{_table("clientes")}` cl ON c.cliente_id = cl.cliente_id
        JOIN `{_table("vehiculos")}` v ON c.vehiculo_id = v.vehiculo_id
        WHERE c.estado = 'CITA_AGENDADA'
          AND c.fecha BETWEEN @start_date AND @end_date
        ORDER BY c.fecha ASC
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end),
            ]
        )
    )
    return [dict(r) for r in job.result()]
