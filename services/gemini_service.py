import os
import json
import re
from google.oauth2 import service_account
import vertexai
from vertexai.generative_models import GenerativeModel

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

def _get_credentials():
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("Falta GOOGLE_CREDENTIALS_JSON")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    return service_account.Credentials.from_service_account_info(info)

def analizar_problema(descripcion: str):
    credentials = _get_credentials()
    vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
    model = GenerativeModel(GEMINI_MODEL)

    prompt = f"""
Eres un asesor de servicio automotriz.
Analiza la siguiente descripción de un cliente y responde SOLO en este formato exacto:

CATEGORIA: <texto corto>
SERVICIO: <texto corto>
ESTIMADO_MIN: <numero entero>
ESTIMADO_MAX: <numero entero>
EXPLICACION: <1 o 2 oraciones breves>

Descripción del cliente:
{descripcion}

Reglas:
- No pongas símbolos de moneda.
- Usa rangos razonables en pesos mexicanos.
- Si no estás seguro, da un rango amplio pero creíble.
- No inventes datos técnicos excesivos.
"""

    response = model.generate_content(prompt)
    text = response.text.strip()

    categoria = _extract(text, r"CATEGORIA:\s*(.+)")
    servicio = _extract(text, r"SERVICIO:\s*(.+)")
    estimado_min = _extract_num(text, r"ESTIMADO_MIN:\s*(\d+)")
    estimado_max = _extract_num(text, r"ESTIMADO_MAX:\s*(\d+)")
    explicacion = _extract(text, r"EXPLICACION:\s*(.+)")

    return {
        "categoria": categoria or "servicio general",
        "servicio": servicio or "revisión general",
        "estimado_min": estimado_min or 800,
        "estimado_max": estimado_max or 1800,
        "explicacion": explicacion or "Se requiere una revisión para confirmar el diagnóstico."
    }

def _extract(text: str, pattern: str):
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip() if match else None

def _extract_num(text: str, pattern: str):
    match = re.search(pattern, text, re.IGNORECASE)
    return int(match.group(1)) if match else None
