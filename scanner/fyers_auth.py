import os
import pyotp
import requests
import base64
from urllib.parse import urlparse, parse_qs
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv

load_dotenv()

def get_access_token():
    """
    Automated login flow for Fyers API v3.
    Uses TOTP and Base64 encoded credentials.
    """
    client_id = os.getenv("FYERS_CLIENT_ID")
    secret_key = os.getenv("FYERS_SECRET_KEY")
    redirect_uri = os.getenv("FYERS_REDIRECT_URI")
    fyers_id = os.getenv("FYERS_ID")
    password = os.getenv("FYERS_PASSWORD")
    totp_secret = os.getenv("FYERS_TOTP_SECRET")

    if not all([client_id, secret_key, redirect_uri, fyers_id, password, totp_secret]):
        raise ValueError("Missing Fyers credentials in environment variables.")

    # Base64 encode fyers_id and password for specific endpoints
    fy_id_b64 = base64.b64encode(fyers_id.encode("ascii")).decode("ascii")
    pin_b64 = base64.b64encode(password.encode("ascii")).decode("ascii")

    session = requests.Session()

    # 1. Send OTP
    # app_id must be "2" for web login simulation
    payload_otp = {
        "fy_id": fy_id_b64,
        "app_id": "2"
    }
    
    res = session.post("https://api-t2.fyers.in/vagator/v2/send_login_otp_v2", json=payload_otp)
    if res.status_code != 200:
        # Try alternate endpoint if the first fails
        res = session.post("https://api.fyers.in/vagator/v2/send_login_otp_v2", json=payload_otp)
        if res.status_code != 200:
            raise Exception(f"Failed to send OTP: {res.text}")

    request_key = res.json().get("request_key")
    if not request_key:
        raise Exception(f"Failed to get request_key: {res.text}")

    # 2. Verify TOTP
    totp = pyotp.TOTP(totp_secret).now()
    payload_verify_otp = {
        "request_key": request_key,
        "otp": totp
    }
    res = session.post("https://api-t2.fyers.in/vagator/v2/verify_otp", json=payload_verify_otp)
    if res.status_code != 200:
        res = session.post("https://api.fyers.in/vagator/v2/verify_otp", json=payload_verify_otp)
        if res.status_code != 200:
            raise Exception(f"Failed to verify OTP: {res.text}")
    
    request_key = res.json().get("request_key")

    # 3. Verify PIN
    payload_verify_pin = {
        "request_key": request_key,
        "identity_type": "pin",
        "identifier": pin_b64
    }
    res = session.post("https://api-t2.fyers.in/vagator/v2/verify_pin_v2", json=payload_verify_pin)
    if res.status_code != 200:
        res = session.post("https://api.fyers.in/vagator/v2/verify_pin_v2", json=payload_verify_pin)
        if res.status_code != 200:
            raise Exception(f"Failed to verify PIN: {res.text}")
    
    access_token_web = res.json().get("data", {}).get("access_token")

    # 4. Generate Auth Code (using v2/token for internal flow)
    # app_id here is the prefix of client_id (before the hyphen)
    app_id_prefix = client_id.split("-")[0] if "-" in client_id else client_id
    
    payload_auth_code = {
        "fyers_id": fyers_id,
        "app_id": app_id_prefix,
        "redirect_uri": redirect_uri,
        "appType": "100",
        "code_challenge": "",
        "state": "abcdefg",
        "scope": "",
        "nonce": "",
        "response_type": "code",
        "create_cookie": True
    }
    headers = {"Authorization": f"Bearer {access_token_web}"}
    res = session.post("https://api.fyers.in/api/v2/token", json=payload_auth_code, headers=headers)
    if res.status_code not in [200, 308] and res.json().get("s") != "ok":
        raise Exception(f"Failed to generate auth code: {res.text}")
    
    auth_url = res.json().get("Url")
    if not auth_url:
        raise Exception(f"No URL returned for auth code generation: {res.text}")

    parsed = urlparse(auth_url)
    auth_code = parse_qs(parsed.query).get("auth_code", [None])[0]
    
    if not auth_code:
        raise Exception("Failed to extract auth code from URL.")

    # 5. Generate Final Fyers API Access Token
    fyers_session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type="code",
        grant_type="authorization_code"
    )
    fyers_session.set_token(auth_code)
    response = fyers_session.generate_token()
    
    if "access_token" not in response:
        raise Exception(f"Failed to generate final access token: {response}")

    return response["access_token"]
