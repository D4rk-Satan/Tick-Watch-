from fyers_apiv3 import fyersModel
import os
from dotenv import load_dotenv

load_dotenv()

def manual_login():
    client_id = os.getenv("FYERS_CLIENT_ID")
    secret_key = os.getenv("FYERS_SECRET_KEY")
    redirect_uri = os.getenv("FYERS_REDIRECT_URI")

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type="code",
        grant_type="authorization_code"
    )

    auth_url = session.generate_authcode()
    print("\n1. Copy this URL into your browser and log in:")
    print("-" * 50)
    print(auth_url)
    print("-" * 50)

    url = input("\n2. After logging in, paste the FULL redirect URL here: ")
    
    # Extract auth_code from the URL
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    auth_code = parse_qs(parsed.query).get("auth_code", [None])[0]

    if not auth_code:
        print("Error: Could not find auth_code in the URL.")
        return

    session.set_token(auth_code)
    response = session.generate_token()

    if "access_token" in response:
        print("\nSUCCESS! Copy the token below:")
        print("-" * 50)
        print(response["access_token"])
        print("-" * 50)
        print("\nPaste this into your AWS .env file as: FYERS_ACCESS_TOKEN=...")
    else:
        print("\nFailed to generate token:", response)

if __name__ == "__main__":
    manual_login()
