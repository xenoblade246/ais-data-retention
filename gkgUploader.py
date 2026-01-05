from urllib.parse import urlparse
import pandas as pd
from elasticsearch.helpers import bulk
import os
import json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import time as tm

################################################ Constants ################################################
GKG_COLUMNS = ["DATE", "NUMARTS", "COUNTS", "THEMES", "LOCATIONS", "PERSONS", "ORGANIZATIONS", "TONE", "CAMEOEVENTIDS", "SOURCES", "SOURCEURLS"]

################################################ Functions ################################################
def connect_to_es(user, password) -> Elasticsearch:
    """
    Establish a connection to the Elasticsearch cluster with timeout and retry logic.

    Returns:
        Elasticsearch: The connected Elasticsearch client.
    """
    return Elasticsearch(
        ["http://localhost:9200"],
        http_auth=(user, password),
        request_timeout=30,
        max_retries=5,
        retry_on_timeout=True
    )

def wait_for_elasticsearch(user: str, password: str, max_retries: int=30, delay: int=5):
    """
    Wait for Elasticsearch to be available before proceeding.

    Returns:
        bool: A boolean representing the success status of Elasticsearch.
    """
    for attempt in range(max_retries):
        try:
            es = Elasticsearch(
                ["http://localhost:9200"],
                basic_auth=(user, password),
                request_timeout=10
            )
            # Test connection
            es.info()
            print(f"Successfully connected to Elasticsearch on attempt {attempt + 1}")
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}: Elasticsearch not ready yet - {e}")
            if attempt < max_retries - 1:
                print(f"Waiting {delay} seconds before retry...")
                tm.sleep(delay)
            else:
                print("Max retries reached. Elasticsearch may not be available.")
                return False
    return False

def read_json(name: str) -> dict:
    """
    Reads the JSON file contents, & outputs the result as a dict.

    Returns:
        dict: The JSON file content.
    """
    with open(name, "r") as f:
        out = json.load(f)
    return out

def gkg_to_dataframe(gkg_file_path, start_row, end_row):
    """
    Convert GKG file to pandas DataFrame
    
    Args:
        gkg_file_path: Path to the GKG file
        sample_size: Number of rows to sample (for testing)
    
    Returns:
        pandas.DataFrame: Converted DataFrame
    """
    print(f"📖 Reading GKG file: {gkg_file_path}")
        
    # GKG files are tab-separated
    rows_to_read = end_row - start_row

    df = pd.read_csv(
        gkg_file_path, 
        sep='\t', 
        header=0,
        skiprows=range(1, start_row),
        nrows=rows_to_read, 
    )
    
    print(f"✅ Successfully loaded {len(df)} rows, {len(df.columns)} columns.")
    print("\nColumn overview:")
    for i, col in enumerate(df.columns):
        print(f"  {i+1:2d}. {col}")
    
    return df

def clean_gkg_data(df):
    """
    Clean and preprocess GKG data based on the 11-column format:
    DATE, NUMARTS, COUNTS, THEMES, LOCATIONS, PERSONS, ORGANIZATIONS, 
    TONE, CAMEOEVENTIDS, SOURCES, SOURCEURLS
    """
    print("🧹 Cleaning GKG data...")
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # 1. Parse DATE column to datetime
    if 'DATE' in df_clean.columns:
        print("  Parsing dates...")
        # GKG dates are usually YYYYMMDDHHMMSS or YYYYMMDD
        # We try to handle both; errors='coerce' turns invalid dates to NaT
        df_clean['DATE'] = pd.to_datetime(df_clean['DATE'].astype(str).str[:8], format='%Y%m%d', errors='coerce')
        df_clean['year'] = df_clean['DATE'].dt.year
        df_clean['month'] = df_clean['DATE'].dt.month
        df_clean['day'] = df_clean['DATE'].dt.day
    
    # 2. Extract domain from SOURCEURLS (Replaces DOCUMENTIDENTIFIER)
    if 'SOURCEURLS' in df_clean.columns:
        print("  Extracting domains from SOURCEURLS...")
        def extract_domain(url_str):
            if pd.isna(url_str):
                return None
            # GKG can have multiple URLs separated by semicolon; take the first one
            primary_url = str(url_str).split(';')[0]
            try:
                parsed = urlparse(primary_url)
                domain = parsed.netloc
                if domain.startswith('www.'):
                    domain = domain[4:]
                return domain
            except Exception:
                return None
        
        df_clean['DOMAIN'] = df_clean['SOURCEURLS'].apply(extract_domain)
    
    # 3. Parse COUNTS field
    if 'COUNTS' in df_clean.columns:
        print("  Parsing counts...")
        def parse_counts(count_str):
            if pd.isna(count_str):
                return []
            counts = []
            for item in str(count_str).split(';'):
                if item.strip():
                    parts = item.split(',')
                    if len(parts) >= 3:
                        counts.append({
                            'type': parts[0],
                            'count': int(parts[1]) if parts[1].isdigit() else 0,
                            'object': parts[2]
                        })
            return counts
        
        df_clean['PARSED_COUNTS'] = df_clean['COUNTS'].apply(parse_counts)

    # 4. Parse THEMES field
    if 'THEMES' in df_clean.columns:
        print("  Parsing themes...")
        def parse_themes(themes_str):
            if pd.isna(themes_str):
                return []
            # Returns a list of themes for the PARSED_THEMES keyword field
            return [theme.strip() for theme in str(themes_str).split(';') if theme.strip()]
        
        df_clean['PARSED_THEMES'] = df_clean['THEMES'].apply(parse_themes)

    # 5. Parse LOCATIONS field
    if 'LOCATIONS' in df_clean.columns:
        print("  Parsing locations...")
        def parse_locations(loc_str):
            if pd.isna(loc_str):
                return []
            locations = []
            for item in str(loc_str).split(';'):
                if item.strip():
                    parts = item.split('#')
                    if len(parts) >= 7:
                        locations.append({
                            'type': parts[0],
                            'full_name': parts[1],
                            'country_code': parts[2],
                            'adm1': parts[3],
                            'lat': float(parts[4]) if parts[4] else 0.0,
                            'lon': float(parts[5]) if parts[5] else 0.0,
                            'feature_id': parts[6]
                        })
            return locations
        
        df_clean['PARSED_LOCATIONS'] = df_clean['LOCATIONS'].apply(parse_locations)

    # 6. Parse TONE field (Replaces V2TONE)
    if 'TONE' in df_clean.columns:
        print("  Parsing tone analysis...")
        def parse_tone(tone_str):
            if pd.isna(tone_str):
                return {}
            parts = str(tone_str).split(',')
            if len(parts) >= 6:
                return {
                    'avg_tone': float(parts[0]),
                    'positive_score': float(parts[1]),
                    'negative_score': float(parts[2]),
                    'polarity': float(parts[3]),
                    'activity_ref_density': float(parts[4]),
                    'self_group_ref_density': float(parts[5])
                }
            return {}
        
        df_clean['PARSED_TONE'] = df_clean['TONE'].apply(parse_tone)

    print(f"✅ Cleaning complete. Original shape: {df.shape}, Cleaned shape: {df_clean.shape}")
    
    return df_clean

def create_es_mapping_for_gkg():
    """Create Elasticsearch mapping for GKG data"""
    return read_json("gkg_mapping.json")

def ingest_gkg_direct(es_client, df, index_name):
    # Convert NaT/NaN to None (JSON null)
    df_ingest = df.where(pd.notnull(df), None)
    docs = df_ingest.to_dict('records')
    
    def generate_actions():
        for doc in docs:
            # 1. DATE: Convert Timestamp to ISO string for ES
            if doc.get('DATE') and hasattr(doc['DATE'], 'isoformat'):
                doc['DATE'] = doc['DATE'].isoformat()
            else:
                doc['DATE'] = None

            # 2. FIX: Don't create a 'Location' field. 
            # Use 'PARSED_LOCATIONS' which is already in your mapping.
            # Ensure it is a list (for nested type)
            if not isinstance(doc.get('PARSED_LOCATIONS'), list):
                doc['PARSED_LOCATIONS'] = []

            # 3. Handle PARSED_COUNTS (ensure it's a list for nested)
            if not isinstance(doc.get('PARSED_COUNTS'), list):
                doc['PARSED_COUNTS'] = []

            yield {
                "_index": index_name,
                "_source": doc
            }

    try:
        success, failed = bulk(es_client, generate_actions())
        print(f"✅ Successfully indexed {success} documents. Failed: {failed}")
    except Exception as e:
        print(f"❌ Bulk indexing failed: {e}")

def upload_gkg_to_elasticsearch(user, password, gkg_file_path, start_row, end_row, index_name="gkg_data"):
    df_raw = gkg_to_dataframe(gkg_file_path, start_row, end_row)
    df_cleaned = clean_gkg_data(df_raw) # This function is great, keep it!
    
    es = connect_to_es(user, password)
    
    # Use the rich GKG mapping you already wrote
    gkg_mapping = create_es_mapping_for_gkg()
    es.indices.create(index=index_name, body=gkg_mapping, ignore=400) # type: ignore
    
    # Step 5: Direct Ingest (No intermediate CSV!)
    ingest_gkg_direct(es, df_cleaned, index_name)

if __name__ == "__main__":
    load_dotenv()
    user = os.getenv("ESUSER")
    password = os.getenv("ESPASSWORD")
    while True:
        start_row, end_row = tuple(input("Please enter start & end rows, separated by a space: ").split(" "))
        upload_gkg_to_elasticsearch(
            user=user,
            password=password,
            gkg_file_path="20251110.gkg.csv",
            start_row = int(start_row),
            end_row = int(end_row),
            index_name="gkg_data"
        )

        if input("Create another data set? Y/N: ") == "N":
            break