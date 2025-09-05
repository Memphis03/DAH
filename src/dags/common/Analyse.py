# --- ANALYTICS : crée des KPI à partir de clean_data ---
import os
import pandas as pd
from datetime import datetime
from extract import RAW_DATA_DIR
from transform import transform_products, transform_clients, transform_orders



BASE_DIR = "/mnt/c/Users/mount/ecommerce-analytics/ecommerce-analytics"
ANALYTICS_DIR = os.path.join(BASE_DIR, "data", "analytics")

def compute_daily_stock(clean_products_file: str):
    df = pd.read_csv(clean_products_file)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "stock" not in df.columns:
        print("La colonne 'stock' est absente du fichier produits nettoyé.")
        return None
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0)


    # Stock total journalier
    daily_stock = df.groupby("date")["stock"].sum().reset_index(name="total_stock")

    out_dir = os.path.join(ANALYTICS_DIR, "stock")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "daily_stock.csv")
    daily_stock.to_csv(out_path, index=False)
    print(f"Stock journalier sauvegardé : {out_path}")

    return out_path


def compute_new_customers(clean_clients_file: str):
    df = pd.read_csv(clean_clients_file)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "customer_id" not in df.columns:
        print("La colonne 'customer_id' est absente du fichier clients nettoyé.")
        return None
    df["customer_id"] = df["customer_id"].astype(str)
    df = df.drop_duplicates(subset=["customer_id", "date"])

    # Première apparition d’un client
    first_purchase = df.groupby("customer_id")["date"].min().reset_index()
    first_purchase["is_new_customer"] = True

    # Nb de nouveaux clients par jour
    daily_new_customers = first_purchase.groupby("date").size().reset_index(name="new_customers")

    out_dir = os.path.join(ANALYTICS_DIR, "clients")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "new_customers.csv")
    daily_new_customers.to_csv(out_path, index=False)
    print(f"Nouveaux clients sauvegardés : {out_path}")

    return out_path


def compute_monthly_revenue(clean_orders_file: str):
    df = pd.read_csv(clean_orders_file)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    if "quantity" not in df.columns or "price" not in df.columns:
        print("Les colonnes 'quantity' et/ou 'price' sont absentes du fichier commandes nettoyé.")
        return None
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    if "total_price" not in df.columns:
        df["total_price"] = df["quantity"] * df["price"]

    df["month"] = df["order_date"].dt.to_period("M")

    monthly_ca = df.groupby("month")["total_price"].sum().reset_index(name="monthly_revenue")

    out_dir = os.path.join(ANALYTICS_DIR, "orders")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "monthly_revenue.csv")
    monthly_ca.to_csv(out_path, index=False)
    print(f"Chiffre d’affaires mensuel sauvegardé : {out_path}")

    return out_path

if __name__=="__main__":
    date = datetime.strptime("2024-05-10", "%Y-%m-%d")
    clean_products_file = transform_products(date)
    if clean_products_file:
        compute_daily_stock(clean_products_file)
    clean_clients_file = transform_clients(date)
    if clean_clients_file:
        compute_new_customers(clean_clients_file)
    clean_orders_file = transform_orders(date)
    if clean_orders_file:
        compute_monthly_revenue(clean_orders_file) 
