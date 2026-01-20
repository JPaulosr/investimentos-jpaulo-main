# utils/telegram.py
import requests

def send_telegram_message(bot_token: str, chat_id: str, text: str, image_url: str = None) -> bool:
    """
    Envia mensagem no Telegram.
    1. Tenta enviar com FOTO (sendPhoto).
    2. Se falhar (ou não tiver foto), envia TEXTO (sendMessage).
    """
    if not bot_token or not chat_id:
        return False
        
    # TENTATIVA 1: Enviar com FOTO
    if image_url and str(image_url).startswith("http"):
        try:
            url_photo = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            payload_photo = {
                "chat_id": chat_id,
                "caption": text,
                "parse_mode": "HTML",
                "photo": image_url
            }
            # Timeout curto para não travar o app se a imagem for pesada
            r = requests.post(url_photo, data=payload_photo, timeout=10)
            
            if r.status_code == 200:
                return True
            else:
                print(f"⚠️ Erro Telegram (Foto): {r.text} - Tentando enviar só texto...")
        except Exception as e:
            print(f"⚠️ Erro Conexão (Foto): {e}")

    # TENTATIVA 2 (Fallback): Enviar apenas TEXTO
    try:
        url_text = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload_text = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        r = requests.post(url_text, data=payload_text, timeout=10)
        return r.ok
    except Exception as e:
        print(f"❌ Erro Crítico Telegram: {e}")
        return False