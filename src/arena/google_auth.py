from __future__ import annotations

from pathlib import Path
from os import getenv
import json

import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow

from arena.config import ROOT


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def load_google_credentials() -> Credentials:
    service_account_path = getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if service_account_path and Path(service_account_path).exists():
        return ServiceAccountCredentials.from_service_account_file(service_account_path, scopes=SCOPES)
    oauth_client_path = getenv("GOOGLE_OAUTH_CLIENT_FILE")
    if oauth_client_path and Path(oauth_client_path).exists():
        return _load_oauth_credentials(Path(oauth_client_path))
    raise RuntimeError("No Google credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or provide google.json.")


def authorized_gspread_client():
    return gspread.authorize(load_google_credentials())


def _load_oauth_credentials(client_path: Path) -> Credentials:
    token_path = ROOT / "data" / "google_token.json"
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        if creds and creds.valid:
            return creds
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
            return creds
    flow = InstalledAppFlow.from_client_secrets_file(str(client_path), SCOPES)
    creds = flow.run_local_server(port=0)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds
