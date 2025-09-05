import os
import io
from datetime import datetime
import pandas as pd
import sqlite3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "/mnt/c/Users/mount/ecommerce-analytics/ecommerce-analytics/clear-canyon-470701-h2-7943eb5a86a8.json"
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")

def connect_to_drive():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileExistsError(f"Credentials file not found at {SERVICE_ACCOUNT_FILE}")
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

def download_file(service, file_id, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Téléchargé {int(status.progress() * 100)}%")
    fh.seek(0)
    with open(local_path, 'wb') as f:
        f.write(fh.read())
    print(f"Fichier téléchargé et renommé dans : {local_path}")
    return local_path, fh

def extract_products(date: datetime, service=None):
    if service is None:
        service = connect_to_drive()

    filename = "products.csv"
    print(f"Recherche du fichier : {filename}")
    
    # Recherche du fichier sur Drive
    files = service.files().list(
        q=f"name='{filename}' and mimeType!='application/vnd.google-apps.folder'",
        spaces='drive',
        fields='files(id, name)'
    ).execute().get('files', [])
    
    if not files:
        print(f"Aucun fichier trouvé avec le nom {filename}.")
        return

    file_id = files[0]['id']
    print(f"Fichier trouvé : {files[0]['name']} (ID : {file_id})")

    # Définition du chemin de sauvegarde
    local_path = os.path.join(RAW_DATA_DIR, f"products/{date.year}/{date.month}/{date.day}.csv")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Téléchargement du fichier directement sur disque
    request = service.files().get_media(fileId=file_id)
    with open(local_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Téléchargé {int(status.progress() * 100)}%")
    print(f"Fichier téléchargé et sauvegardé dans : {local_path}")

    # Lecture et filtrage par date depuis le fichier téléchargé
    data = pd.read_csv(local_path)
    final_data = data[data.date == date.strftime("%Y-%m-%d")]
    
    if not final_data.empty:
        final_data.to_csv(local_path, index=False)
        print("Données filtrées par date :")
        print(final_data.head())
    else:
        print(f"Aucune donnée trouvée pour la date {date.strftime('%Y-%m-%d')}")

def extract_clients(date: datetime, service=None):
    if service is None:
        service = connect_to_drive()
    FOLDER = "clients"
    folder_exist = service.files().list(
        q=f"name='{FOLDER}' and mimeType='application/vnd.google-apps.folder'",
        spaces='drive',
        fields='files(id, name)'
    ).execute().get('files', [])
    if not folder_exist:
        print(f"Dossier {FOLDER} non trouvé sur Drive.")
        return
    filename = f"clients_{date.strftime('%Y-%m-%d')}.csv"
    files = service.files().list(
        q=f"name='{filename}' and mimeType!='application/vnd.google-apps.folder'",
        spaces='drive',
        fields='files(id, name)'
    ).execute().get('files', [])
    if not files:
        print(f"Aucun fichier trouvé avec le nom {filename}.")
        return
    file_id = files[0]['id']
    print(f"Fichier trouvé : {files[0]['name']} (ID : {file_id})")
    local_path = os.path.join(RAW_DATA_DIR, f"clients/{date.year}/{date.month}/{date.day}.csv")
    download_file(service, file_id, local_path)

def extract_orders(date: datetime, db_path="ecommerce_orders_may2024.db", table_name="ecommerce_orders"):
    conn = sqlite3.connect(db_path)
    try:
        date_str = date.strftime("%Y-%m-%d")
        df = pd.read_sql_query(f'SELECT * FROM {table_name} WHERE order_date="{date_str}"', conn)
    finally:
        conn.close()
    if not df.empty:
        local_path = os.path.join(RAW_DATA_DIR, f"orders/{date.year}/{date.month}/{date.day}.csv")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_csv(local_path, index=False)
        print(f"Fichier commandes sauvegardé : {local_path}")
    else:
        print(f"Aucune commande trouvée pour {date.strftime('%Y-%m-%d')}")

if __name__=="__main__":
    service = connect_to_drive()
    date_to_extract = datetime.strptime("2024-05-05", "%Y-%m-%d")
    extract_products(date_to_extract, service=service)
    extract_clients(date_to_extract, service=service)
    extract_orders(date_to_extract)
