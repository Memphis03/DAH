# --- TRANSFORM: lit raw_data et écrit dans clean_data ---

import os
from datetime import datetime   
import pandas as pd
from extract import RAW_DATA_DIR

BASE_DIR = "/mnt/c/Users/mount/ecommerce-analytics/ecommerce-analytics"
CLEAN_DATA_DIR = os.path.join(BASE_DIR, "data", "cleane_data")


def transform_products(date: datetime):
    raw_path = os.path.join(RAW_DATA_DIR, "products", str(date.year), str(date.month), f"{date.day}.csv")
    if not os.path.exists(raw_path):
        print(f"Fichier produits brut introuvable : {raw_path}")
        return None
    
    df = pd.read_csv(raw_path)
    df.columns = [c.strip().lower() for c in df.columns]
    # Typage léger
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.drop_duplicates()
        # Exemples de normalisation
    for col in ("product_id", "item_id"):
        if col in df.columns:
            df[col] = df[col].astype(str)
            break

    out_dir = os.path.join(CLEAN_DATA_DIR, "products", str(date.year), str(date.month))
    os.makedirs(out_dir, exist_ok=True) 
    out_path = os.path.join(out_dir, f"{date.day}.csv")
    df.to_csv(out_path, index=False)
    print(f"Produits nettoyés : {out_path}")
    return out_path


def transform_clients(date: datetime):
    raw_path = os.path.join(RAW_DATA_DIR, "clients", str(date.year), str(date.month), f"{date.day}.csv")
    if not os.path.exists(raw_path):
        print(f"Fichier clients brut introuvable : {raw_path}")
        return None


    df = pd.read_csv(raw_path)
    df.columns = [c.strip().lower() for c in df.columns]
    # Exemples de normalisation
    for col in ("client_id", "customer_id"):
        if col in df.columns:
            df[col] = df[col].astype(str)
            break
    df = df.drop_duplicates()
    # Typage léger
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")    
    
    out_dir = os.path.join(CLEAN_DATA_DIR, "clients", str(date.year), str(date.month))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{date.day}.csv")
    df.to_csv(out_path, index=False)
    print(f"Clients nettoyés : {out_path}")
    return out_path


def transform_orders(date: datetime):
    raw_path = os.path.join(RAW_DATA_DIR, "orders", str(date.year), str(date.month), f"{date.day}.csv")
    if not os.path.exists(raw_path):
        print(f"Fichier commandes brut introuvable : {raw_path}")
        return None

    df = pd.read_csv(raw_path)
    df.columns = [c.strip().lower() for c in df.columns]

    # Typage
    for dcol in ("order_date", "date"):
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
            break

    # Indics utiles
    if "quantity" in df.columns and "price" in df.columns and "total_price" not in df.columns:
        df["total_price"] = df["quantity"] * df["price"]

    df = df.drop_duplicates()

    out_dir = os.path.join(CLEAN_DATA_DIR, "orders", str(date.year), str(date.month))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{date.day}.csv")
    df.to_csv(out_path, index=False)
    print(f"Commandes nettoyées : {out_path}")
    return out_path


if __name__=="__main__":
    date_to_transform = datetime.strptime("2024-05-01", "%Y-%m-%d")
    transform_products(date_to_transform)
    transform_clients(date_to_transform)
    transform_orders(date_to_transform)
