import os
import base64
import requests
from dotenv import load_dotenv
from urllib.parse import urlencode, urlparse, parse_qs

class Reauth:
    auth_url = 'https://accounts.spotify.com/authorize?'
    token_url = 'https://accounts.spotify.com/api/token'
    
    def __init__(self):
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')    
        self.redirect_uri = 'http://127.0.0.1:8888/callback'

    def extract_token_code(self, url: str) -> str:
        url = url.strip()
        code_query = parse_qs(urlparse(url).query)

        if "code" not in code_query:
            raise ValueError("Authorization code not found in the URL.")

        return code_query["code"][0]

    def exchange_token_code(self, code: str) -> list:
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_header = base64.b64encode(auth_str.encode()).decode()

        headers = {
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri
        }

        response = requests.post(
            self.token_url,
            headers=headers,
            data=data
        )

        response_data = response.json()
        return [response_data["access_token"], response_data["refresh_token"]]
    
    def write_token_code(self, refresh_token:str) -> None:
        os.environ["SPOTIFY_REFRESH_TOKEN"] = refresh_token
        print("Successfully wrote refresh_token to .env file")
        pass

    def reauth(self, token_code: str) -> None:
        params = {
            "client_id": self.client_id,
            "response_type": 'code',
            "redirect_uri": self.redirect_uri,
            "scope": 'user-read-recently-played'
        }

        auth_request_url = f"{self.auth_url}{urlencode(params)}"

        print("\nOpen this URL in your browser:\n")
        print(auth_request_url)

        token_url = input("\n Authorize, then paste the entire url here:\n").strip()
        token_code = self.extract_token_code(token_url)

        access_token, refresh_token = self.exchange_token_code(token_code)

        self.write_token_code(refresh_token)

if __name__ == "__main__":
    reauth = Reauth()
    reauth.reauth()