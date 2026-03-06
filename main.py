import os
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import requests

from services.bigquery_service import (
    log_conversation,
    get_cliente_by_telegram_id,
    create_cliente,
    create_vehiculo,
    create_cita,
    get_upcoming_citas,
)
from services.gemini_service import analizar_problema
from services.calendar_service import create_calendar_event

app = Flask(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
REMINDER_SECRET = os.getenv("REMINDER_SECRET", "demo-secret")

# Estado simple en memoria para la demo
USER_STATE = {}

def send_telegram_message(chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=30)

def get_slots():
    now = datetime.now(timezone.utc)
    tomorrow = now + timedelta(days=1)

    s1 = tomorrow.replace(hour=16, minute=0, second=0, microsecond=0)
    s2 = tomorrow.replace(hour=18, minute=0, second=0, microsecond=0)
    s3 = tomorrow.replace(day=tomorrow.day + 1, hour=16, minute=0, second=0, microsecond=0)

    return {
        "1": s1,
        "2": s2,
        "3": s3,
    }

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/")
def telegram_webhook():
    data = request.get_json(force=True, silent=True) or {}

    message = data.get("message")
    if not message:
        return "ok", 200

    chat_id = str(message["chat"]["id"])
    text = message.get("text", "").strip()

    if not text:
        return "ok", 200

    log_conversation(chat_id, "user", text)

    cliente = get_cliente_by_telegram_id(chat_id)
    state = USER_STATE.get(chat_id, {})

    # Cliente nuevo
    if not cliente and not state:
        USER_STATE[chat_id] = {"step": "ASK_NAME"}
        reply = "Hola 👋 Soy el asistente del taller.\n\nPara comenzar, ¿me compartes tu nombre?"
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    # Registro de nombre
    if state.get("step") == "ASK_NAME":
        cliente = create_cliente(chat_id, text)
        USER_STATE[chat_id] = {"step": "ASK_MARCA", "cliente": cliente}
        reply = f"Gracias, {cliente['nombre']} ✅\n\nAhora dime la marca de tu vehículo."
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    # Cliente existente sin flujo activo
    if cliente and not state:
        USER_STATE[chat_id] = {"step": "ASK_MARCA", "cliente": cliente}
        reply = f"Hola {cliente['nombre']} 👋\n\nVamos a registrar tu vehículo.\n¿Cuál es la marca?"
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    # Flujo vehículo
    if state.get("step") == "ASK_MARCA":
        state["marca"] = text
        state["step"] = "ASK_MODELO"
        USER_STATE[chat_id] = state
        reply = "Perfecto. ¿Cuál es el modelo?"
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    if state.get("step") == "ASK_MODELO":
        state["modelo"] = text
        state["step"] = "ASK_ANIO"
        USER_STATE[chat_id] = state
        reply = "¿De qué año es?"
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    if state.get("step") == "ASK_ANIO":
        try:
            anio = int(text)
        except ValueError:
            reply = "Por favor escribe el año en número. Ejemplo: 2019"
            send_telegram_message(chat_id, reply)
            log_conversation(chat_id, "bot", reply)
            return "ok", 200

        state["anio"] = anio
        state["step"] = "ASK_KM"
        USER_STATE[chat_id] = state
        reply = "¿Cuál es el kilometraje aproximado? Si no lo sabes, escribe 0."
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    if state.get("step") == "ASK_KM":
        try:
            km_actual = int(text)
        except ValueError:
            km_actual = 0

        vehiculo = create_vehiculo(
            cliente_id=state["cliente"]["cliente_id"],
            marca=state["marca"],
            modelo=state["modelo"],
            anio=state["anio"],
            km_actual=km_actual,
        )
        state["vehiculo"] = vehiculo
        state["step"] = "ASK_PROBLEMA"
        USER_STATE[chat_id] = state
        reply = "Muy bien. Ahora cuéntame qué problema tiene tu vehículo o qué servicio necesitas."
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    if state.get("step") == "ASK_PROBLEMA":
        analisis = analizar_problema(text)
        state["problema"] = text
        state["analisis"] = analisis
        state["step"] = "ASK_AGENDAR"
        USER_STATE[chat_id] = state

        reply = (
            f"{analisis['explicacion']}\n\n"
            f"Categoría detectada: {analisis['categoria']}\n"
            f"Servicio sugerido: {analisis['servicio']}\n"
            f"Estimado: ${analisis['estimado_min']} - ${analisis['estimado_max']} MXN\n\n"
            f"¿Te gustaría agendar una cita? Responde SI o NO."
        )
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    if state.get("step") == "ASK_AGENDAR":
        if text.upper() not in ["SI", "SÍ", "NO"]:
            reply = "Por favor responde SI o NO."
            send_telegram_message(chat_id, reply)
            log_conversation(chat_id, "bot", reply)
            return "ok", 200

        if text.upper() == "NO":
            reply = "Entendido. Cuando quieras retomar el proceso, escríbeme de nuevo."
            USER_STATE.pop(chat_id, None)
            send_telegram_message(chat_id, reply)
            log_conversation(chat_id, "bot", reply)
            return "ok", 200

        slots = get_slots()
        state["slots"] = {k: v.isoformat() for k, v in slots.items()}
        state["step"] = "ASK_SLOT"
        USER_STATE[chat_id] = state

        reply = (
            "Tengo estos horarios disponibles:\n"
            f"1) {slots['1'].astimezone().strftime('%d/%m %H:%M')}\n"
            f"2) {slots['2'].astimezone().strftime('%d/%m %H:%M')}\n"
            f"3) {slots['3'].astimezone().strftime('%d/%m %H:%M')}\n\n"
            "Responde 1, 2 o 3."
        )
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    if state.get("step") == "ASK_SLOT":
        if text not in ["1", "2", "3"]:
            reply = "Por favor elige 1, 2 o 3."
            send_telegram_message(chat_id, reply)
            log_conversation(chat_id, "bot", reply)
            return "ok", 200

        fecha_iso = state["slots"][text]
        vehiculo_texto = f"{state['vehiculo']['marca']} {state['vehiculo']['modelo']} {state['vehiculo']['anio']}"
        calendar_event_id = create_calendar_event(
            nombre_cliente=state["cliente"]["nombre"],
            vehiculo_texto=vehiculo_texto,
            motivo=state["problema"],
            fecha_inicio_iso=fecha_iso,
        )

        cita = create_cita(
            cliente_id=state["cliente"]["cliente_id"],
            vehiculo_id=state["vehiculo"]["vehiculo_id"],
            fecha_iso=fecha_iso,
            motivo=state["problema"],
            categoria=state["analisis"]["categoria"],
            estimado_min=state["analisis"]["estimado_min"],
            estimado_max=state["analisis"]["estimado_max"],
            calendar_event_id=calendar_event_id,
        )

        dt = datetime.fromisoformat(fecha_iso)
        reply = (
            f"Tu cita quedó agendada ✅\n\n"
            f"Fecha: {dt.strftime('%d/%m/%Y %H:%M')}\n"
            f"Vehículo: {vehiculo_texto}\n"
            f"Folio de cita: {cita['cita_id']}\n\n"
            f"Te enviaremos recordatorios antes de tu cita."
        )
        USER_STATE.pop(chat_id, None)
        send_telegram_message(chat_id, reply)
        log_conversation(chat_id, "bot", reply)
        return "ok", 200

    reply = "Ocurrió algo inesperado. Escribe cualquier mensaje para comenzar de nuevo."
    USER_STATE.pop(chat_id, None)
    send_telegram_message(chat_id, reply)
    log_conversation(chat_id, "bot", reply)
    return "ok", 200

@app.post("/cron/send-reminders")
def send_reminders():
    secret = request.headers.get("X-Reminder-Secret")
    if secret != REMINDER_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    citas = get_upcoming_citas(days_ahead=3)

    enviados = 0
    for cita in citas:
        fecha = cita["fecha"]
        if hasattr(fecha, "strftime"):
            fecha_txt = fecha.strftime("%d/%m/%Y %H:%M")
        else:
            fecha_txt = str(fecha)

        mensaje = (
            f"Recordatorio de cita 🚗\n\n"
            f"Hola {cita['nombre']}, te recordamos que tienes una cita próxima el {fecha_txt}.\n"
            f"Vehículo: {cita['marca']} {cita['modelo']} {cita['anio']}\n"
            f"Motivo: {cita['motivo']}"
        )
        send_telegram_message(cita["telegram_id"], mensaje)
        enviados += 1

    return jsonify({"ok": True, "enviados": enviados})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
