import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def test_telegram():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Error: Faltan TELEGRAM_TOKEN o TELEGRAM_CHAT_ID en el .env")
        return

    print(f"Enviando mensaje de prueba a Chat ID: {TELEGRAM_CHAT_ID}...")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID, 
        "text": "🚀 *¡Conexión Exitosa!*\n\nEl bot de Scalping v4 ya está vinculado a este chat y te avisará cuando detecte oportunidades.", 
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("✅ Mensaje enviado con éxito. Revisa tu Telegram.")
        else:
            print(f"❌ Error de API: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    test_telegram()
