import pandas as pd
import glob
import os
import re


RAW_DIR = "data/raw/kaggle"
OUT_PATH = "data/processed/all_kaggle_data.csv"

def normalize_columns(df):

    df = df.copy()
    new_cols = {}

    for col in df.columns:
        clean = col.lower()
        clean = clean.replace(" ", "_")
        clean = re.sub(r"[^a-z0-9_]+", "", clean)
        clean = clean.strip("_")
        new_cols[col] = clean

    df.rename(columns=new_cols, inplace=True)
    return df

def load_file(path):
    try:
        if path.endswith(".csv"):
            df = pd.read_csv(path)
        elif path.endswith(".xlsx"):
            df = pd.read_excel(path)
        else:
            print(f"Skipping unsupported file: {path}")
            return None


        df = normalize_columns(df)
        return df

    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def merge_all():

    files = glob.glob(os.path.join(RAW_DIR, "*"))

    if not files:
        print("No files found in data/raw/kaggle/")
        return

    print("Found files:")
    for f in files:
        print(" -", f)

    dfs = []
    for f in files:
        df = load_file(f)
        if df is not None:
            print(f"Loaded {os.path.basename(f)} -> shape {df.shape}")
            dfs.append(df)

    if not dfs:
        print("No valid data files to merge.")
        return

    print("\nConcatenating datasets vertically...")
    combined = pd.concat(dfs, ignore_index=True)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    combined.to_csv(OUT_PATH, index=False)

    print(f"\nSaved merged dataset → {OUT_PATH}")
    print(f"Final shape: {combined.shape}")


if __name__ == "__main__":
    merge_all()
