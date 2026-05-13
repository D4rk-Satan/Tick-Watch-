import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_telegram():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    print(f"Testing with Token: {token[:5]}... and Chat ID: {chat_id}")
    
    message = "🔔 TEST: Your Fyers Scanner is connected to Telegram!"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("✅ SUCCESS: Check your Telegram!")
        else:
            print(f"❌ FAILED: {response.text}")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    test_telegram()
