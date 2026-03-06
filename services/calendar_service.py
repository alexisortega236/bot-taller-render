import os
import json
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
CALENDAR_ID = os.getenv("CALENDAR_ID")

SCOPES = ["https://www.googleapis.com/auth/calendar"]

def _get_credentials():
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("Falta GOOGLE_CREDENTIALS_JSON")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def create_calendar_event(nombre_cliente: str, vehiculo_texto: str, motivo: str, fecha_inicio_iso: str):
    credentials = _get_credentials()
    service = build("calendar", "v3", credentials=credentials)

    start_dt = datetime.fromisoformat(fecha_inicio_iso.replace("Z", "+00:00"))
    end_dt = start_dt + timedelta(hours=1)

    event = {
        "summary": f"Cita Taller - {nombre_cliente}",
        "description": f"Vehículo: {vehiculo_texto}\nMotivo: {motivo}",
        "start": {
            "dateTime": start_dt.isoformat(),
            "timeZone": "America/Mexico_City",
        },
        "end": {
            "dateTime": end_dt.isoformat(),
            "timeZone": "America/Mexico_City",
        },
    }

    created_event = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
    return created_event["id"]
