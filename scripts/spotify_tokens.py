import base64
import os
import requests
from dotenv import load_dotenv

class Tokens:
    token_url = 'https://accounts.spotify.com/api/token'

    def __init__(self):
        load_dotenv()

        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        self.refresh_token = os.getenv('SPOTIFY_REFRESH_TOKEN')

    def get_access_token(self):
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

        response = requests.post(
            self.token_url,
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token
            },
            headers = {
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        if response.status_code != 200:
            try:
                err = response.json()
            except ValueError:
                err = {}
            if err.get('error') == 'invalid_grant':
                print("ERROR: refresh token has expired, you'll need to reauthenticate the app")
            response.raise_for_status()
        
        response_json = response.json()
        access_token = response_json["access_token"]

        return response_json["access_token"]