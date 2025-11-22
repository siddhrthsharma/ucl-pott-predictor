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
            return None

        df = normalize_columns(df)
        return df

    except Exception as e:
        return None

def process_attacking_file(df):
    """
    Process attacking.csv file specifically for 21/22 season data.
    Adds season information and renames columns to match the main dataset.
    """
    df = df.copy()
    
    # Add season information for 21/22
    df['season_year'] = '21/22'
    df['season'] = 'UEFA Champions League 21/22'
    
    # Rename columns to match processed file structure
    df = df.rename(columns={
        'player_name': 'name',
        'club': 'team',
        'match_played': 'matchesstarted'
    })
    
    # Add specific goal corrections for known players (e.g., Benzema scored 15 goals in 21/22)
    # This can be expanded for other players if needed
    if 'name' in df.columns:
        df.loc[df['name'].str.contains('Benzema', case=False, na=False), 'goals'] = 15
    
    return df

def merge_all():
    files = glob.glob(os.path.join(RAW_DIR, "*"))

    if not files:
        print("No files found in", RAW_DIR)
        return

    print("Found files:")
    for f in files:
        print(" -", f)

    dfs = []
    for f in files:
        df = load_file(f)
        if df is not None:
            filename = os.path.basename(f)
            print(f"Loaded {filename} -> shape {df.shape}")
            
            # Special processing for attacking.csv (21/22 season data)
            if 'attacking' in filename.lower():
                print(f"  → Processing {filename} as 21/22 season attacking data")
                df = process_attacking_file(df)
            
            dfs.append(df)

    if not dfs:
        print("No valid dataframes to merge")
        return

    combined = pd.concat(dfs, ignore_index=True, sort=False)
    print(f"\nTotal rows in combined dataset: {len(combined)}")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    combined.to_csv(OUT_PATH, index=False)
    print(f"✓ Successfully saved to {OUT_PATH}")


if __name__ == "__main__":
    merge_all()
