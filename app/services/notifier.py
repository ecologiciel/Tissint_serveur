import os
import httpx

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")

async def send_telegram_radar_alert(scan_id: str, stone_class: str, confidence: float, user_id: str):
    """
    Envoie une alerte administrative via Telegram.
    Exécuté en tâche de fond pour ne pas bloquer l'Event Loop de FastAPI.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ADMIN_CHAT_ID:
        print(f"⚠️ [Telegram] Configuration manquante (Token ou Chat ID). Alerte ignorée pour le scan {scan_id}.")
        return

    message = (
        f"🚨 <b>ALERTE RADAR : Météorite Rare Détectée !</b> 🚨\n\n"
        f"<b>ID du Scan :</b> {scan_id}\n"
        f"<b>Classe de la pierre :</b> {stone_class}\n"
        f"<b>Score de confiance :</b> {confidence:.2%}\n"
        f"<b>Identifiant de l'artisan/chercheur :</b> {user_id}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_ADMIN_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=payload, timeout=5.0)
            if res.status_code != 200:
                print(f"❌ [Telegram] Erreur de l'API ({res.status_code}): {res.text}")
            else:
                print(f"✅ [Telegram] Alerte envoyée (Scan ID: {scan_id})")
    except Exception as e:
        # Ne fait jamais planter l'API principale en cas d'erreur de réseau ou d'indisponibilité de Telegram
        print(f"❌ [Telegram] Exception lors de l'envoi de l'alerte : {e}")
