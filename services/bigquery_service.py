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


def _run_dml(query: str, params: list):
    client = get_bq_client()
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    job.result()
    return True


def log_conversation(telegram_id: str, role: str, mensaje: str):
    query = f"""
        INSERT INTO `{_table("conversaciones")}`
        (event_id, telegram_id, role, mensaje, timestamp)
        VALUES (@event_id, @telegram_id, @role, @mensaje, @timestamp)
    """
    _run_dml(
        query,
        [
            bigquery.ScalarQueryParameter("event_id", "STRING", str(uuid.uuid4())),
            bigquery.ScalarQueryParameter("telegram_id", "STRING", telegram_id),
            bigquery.ScalarQueryParameter("role", "STRING", role),
            bigquery.ScalarQueryParameter("mensaje", "STRING", mensaje),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", datetime.now(timezone.utc)),
        ],
    )


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
    cliente_id = f"CLI-{uuid.uuid4().hex[:10]}"
    query = f"""
        INSERT INTO `{_table("clientes")}`
        (cliente_id, telegram_id, nombre, telefono, created_at)
        VALUES (@cliente_id, @telegram_id, @nombre, @telefono, @created_at)
    """
    _run_dml(
        query,
        [
            bigquery.ScalarQueryParameter("cliente_id", "STRING", cliente_id),
            bigquery.ScalarQueryParameter("telegram_id", "STRING", telegram_id),
            bigquery.ScalarQueryParameter("nombre", "STRING", nombre),
            bigquery.ScalarQueryParameter("telefono", "STRING", telefono),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now(timezone.utc)),
        ],
    )
    return {"cliente_id": cliente_id, "telegram_id": telegram_id, "nombre": nombre}


def create_vehiculo(
    cliente_id: str,
    marca: str,
    modelo: str,
    anio: int,
    placa: str = None,
    km_actual: int = None
):
    vehiculo_id = f"VEH-{uuid.uuid4().hex[:10]}"
    query = f"""
        INSERT INTO `{_table("vehiculos")}`
        (vehiculo_id, cliente_id, marca, modelo, anio, placa, km_actual, created_at)
        VALUES (@vehiculo_id, @cliente_id, @marca, @modelo, @anio, @placa, @km_actual, @created_at)
    """
    _run_dml(
        query,
        [
            bigquery.ScalarQueryParameter("vehiculo_id", "STRING", vehiculo_id),
            bigquery.ScalarQueryParameter("cliente_id", "STRING", cliente_id),
            bigquery.ScalarQueryParameter("marca", "STRING", marca),
            bigquery.ScalarQueryParameter("modelo", "STRING", modelo),
            bigquery.ScalarQueryParameter("anio", "INT64", anio),
            bigquery.ScalarQueryParameter("placa", "STRING", placa),
            bigquery.ScalarQueryParameter("km_actual", "INT64", km_actual),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now(timezone.utc)),
        ],
    )
    return {
        "vehiculo_id": vehiculo_id,
        "cliente_id": cliente_id,
        "marca": marca,
        "modelo": modelo,
        "anio": anio,
    }


def create_cita(
    cliente_id: str,
    vehiculo_id: str,
    fecha_iso: str,
    motivo: str,
    categoria: str,
    estimado_min: float,
    estimado_max: float,
    calendar_event_id: str,
    estado: str = "CITA_AGENDADA"
):
    cita_id = f"CIT-{uuid.uuid4().hex[:10]}"
    fecha_dt = datetime.fromisoformat(fecha_iso.replace("Z", "+00:00"))

    query = f"""
        INSERT INTO `{_table("citas")}`
        (cita_id, cliente_id, vehiculo_id, fecha, motivo, categoria, estimado_min, estimado_max, calendar_event_id, estado, created_at)
        VALUES (@cita_id, @cliente_id, @vehiculo_id, @fecha, @motivo, @categoria, @estimado_min, @estimado_max, @calendar_event_id, @estado, @created_at)
    """
    _run_dml(
        query,
        [
            bigquery.ScalarQueryParameter("cita_id", "STRING", cita_id),
            bigquery.ScalarQueryParameter("cliente_id", "STRING", cliente_id),
            bigquery.ScalarQueryParameter("vehiculo_id", "STRING", vehiculo_id),
            bigquery.ScalarQueryParameter("fecha", "TIMESTAMP", fecha_dt),
            bigquery.ScalarQueryParameter("motivo", "STRING", motivo),
            bigquery.ScalarQueryParameter("categoria", "STRING", categoria),
            bigquery.ScalarQueryParameter("estimado_min", "FLOAT64", estimado_min),
            bigquery.ScalarQueryParameter("estimado_max", "FLOAT64", estimado_max),
            bigquery.ScalarQueryParameter("calendar_event_id", "STRING", calendar_event_id),
            bigquery.ScalarQueryParameter("estado", "STRING", estado),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now(timezone.utc)),
        ],
    )
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