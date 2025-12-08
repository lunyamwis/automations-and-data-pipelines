import os
import io
import json
import requests
import pandas as pd
from io import BytesIO

# Google Drive API
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# -------------------------------
# CONFIG
# -------------------------------

VA_API_KEY = os.getenv("VA_API_KEY")
headers = {"Authorization": f"Basic {VA_API_KEY}"}

GOOGLE_CLIENT_SECRET = "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_ID = "18TWweKvj1wPcKNaJ3FOAmPPZzBMP2XEW"

DOWNLOAD_DIR = "downloaded"
CSV_PATH = "results.csv"

# -------------------------------
# LANDING.AI EXTRACTION SCHEMA
# -------------------------------
SCHEMA_PATH = "schema.json"
with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)
# -------------------------------
# GOOGLE DRIVE AUTHENTICATION
# -------------------------------
creds = None

if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CLIENT_SECRET, SCOPES)
        creds = flow.run_local_server(port=0)

    with open("token.json", "w") as token:
        token.write(creds.to_json())

service = build("drive", "v3", credentials=creds)

# -------------------------------
# LIST FILES IN DRIVE FOLDER
# -------------------------------
query = f"'{FOLDER_ID}' in parents and trashed = false"
results = service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    print("No files found in the Drive folder.")
    exit()

print(f"Found {len(files)} PDF files")

# -------------------------------
# PREP CSV (header once)
# -------------------------------
pd.DataFrame(columns=["filename", "extracted_json"]).to_csv(CSV_PATH, index=False)

df_rows = []
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# -------------------------------
# PROCESS EACH PDF
# -------------------------------
for f in files:
    file_id = f["id"]
    file_name = f["name"]

    # -------------------------------
    # DOWNLOAD PDF
    # -------------------------------
    print(f"\nDownloading: {file_name}")
    request = service.files().get_media(fileId=file_id)
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    with io.FileIO(file_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"  → {int(status.progress() * 100)}%")

    # -------------------------------
    # LANDING.AI PARSE → EXTRACT
    # -------------------------------
    print(f"Extracting data from: {file_name}")

    parse_response = requests.post(
        "https://api.va.landing.ai/v1/ade/parse",
        headers=headers,
        files=[("document", open(file_path, "rb"))],
        data={"model": "dpt-2"},
    )

    markdown_content = parse_response.json().get("markdown", "")

    extract_response = requests.post(
        "https://api.va.landing.ai/v1/ade/extract",
        headers=headers,
        files=[("markdown", BytesIO(markdown_content.encode("utf-8")))],
        data={"schema": json.dumps(schema)},
    )

    extracted_json = extract_response.json()

    # -------------------------------
    # SAVE RESULT TO CSV
    # -------------------------------
    row = {
        "filename": file_name,
        "extracted_json": json.dumps(extracted_json)
    }

    df_rows.append(row)
    pd.DataFrame([row]).to_csv(
        CSV_PATH, mode="a", header=False, index=False
    )

    print(f"✔ Saved extracted data for: {file_name}")

    # -------------------------------
    # DELETE LOCAL PDF AFTER PROCESSING
    # -------------------------------
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"🗑️ Deleted local file: {file_name}")
    else:
        print(f"⚠️ Not found for deletion: {file_name}")

# -------------------------------
# FINAL DATAFRAME IN MEMORY
# -------------------------------
df = pd.DataFrame(df_rows)

print("\nAll files processed and cleaned!")
print(df.head())

