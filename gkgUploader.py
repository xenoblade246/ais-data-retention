import os
import pandas as pd
import json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import time as tm
from urllib.parse import urlparse

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

def parse_gkg_columns():
    """Return the column definitions for GKG 2.0 format"""
    gkg_columns = [
        "GKGRECORDID",          # Unique identifier
        "DATE",                 # Date in YYYYMMDD format
        "SOURCECOLLECTIONIDENTIFIER",  # 1=Web, 2=Citation, 3=Core, 4=DTIC, 5=JSTOR, 6=USGS
        "SOURCECOMMONNAME",     # Common name of source
        "DOCUMENTIDENTIFIER",   # URL or identifier
        # The rest are counts, themes, locations, etc.
        "COUNTS",               # Counts of events, emotions, etc.
        "V2COUNTS",             # Enhanced counts
        "THEMES",               # CAMEO themes
        "V2THEMES",             # Enhanced themes
        "LOCATIONS",            # Locations mentioned
        "V2LOCATIONS",          # Enhanced locations
        "PERSONS",              # Persons mentioned
        "V2PERSONS",            # Enhanced persons
        "ORGANIZATIONS",        # Organizations mentioned
        "V2ORGANIZATIONS",      # Enhanced organizations
        "V2TONE",               # Tone analysis
        "V2ENHANCEDDATES",      # Enhanced dates
        "V2GCAM",               # Geolocation with CAMEO
        "V2SHARINGIMAGE",       # Sharing image URLs
        "V2RELATEDIMAGES",      # Related images
        "V2SOCIALIMAGEEMBEDS",  # Social image embeds
        "V2SOCIALVIDEOEMBEDS",  # Social video embeds
        "V2QUOTATIONS",         # Quotations
        "V2ALLNAMES",           # All names mentioned
        "V2AMOUNTS",            # Monetary amounts
        "V2TRANSLATIONINFO",    # Translation information
        "V2EXTRASXML"           # Extra XML data
    ]
    return gkg_columns

def gkg_to_dataframe(gkg_file_path, sample_size=1000):
    """
    Convert GKG file to pandas DataFrame
    
    Args:
        gkg_file_path: Path to the GKG file
        sample_size: Number of rows to sample (for testing)
    
    Returns:
        pandas.DataFrame: Converted DataFrame
    """
    print(f"📖 Reading GKG file: {gkg_file_path}")
    
    columns = parse_gkg_columns()
    
    # Read the GKG file
    try:
        # GKG files are tab-separated
        if sample_size:
            df = pd.read_csv(gkg_file_path, sep='\t', nrows=sample_size, 
                            header=None, names=columns, low_memory=False)
            print(f"📊 Sampled {sample_size} rows for preview")
        else:
            df = pd.read_csv(gkg_file_path, sep='\t', 
                            header=None, names=columns, low_memory=False)
        
        print(f"✅ Successfully loaded {len(df)} rows, {len(df.columns)} columns")
        print("\nColumn overview:")
        for i, col in enumerate(df.columns):
            print(f"  {i+1:2d}. {col}")
        
        return df
    
    except Exception as e:
        print(f"❌ Error reading GKG file: {e}")
        return None

def clean_gkg_data(df):
    """
    Clean and preprocess GKG data
    
    Args:
        df: Raw GKG DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    print("🧹 Cleaning GKG data...")
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Parse DATE column to datetime
    if 'DATE' in df_clean.columns:
        print("  Parsing dates...")
        df_clean['DATE'] = pd.to_datetime(df_clean['DATE'], format='%Y%m%d', errors='coerce')
        df_clean['year'] = df_clean['DATE'].dt.year
        df_clean['month'] = df_clean['DATE'].dt.month
        df_clean['day'] = df_clean['DATE'].dt.day
    
    # Extract domain from DOCUMENTIDENTIFIER
    if 'DOCUMENTIDENTIFIER' in df_clean.columns:
        print("  Extracting domains...")
        def extract_domain(url):
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        
        df_clean['DOMAIN'] = df_clean['DOCUMENTIDENTIFIER'].apply(extract_domain)
    
    # Parse COUNTS field
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
                            'count': parts[1],
                            'object': parts[2]
                        })
            return counts
        
        df_clean['PARSED_COUNTS'] = df_clean['COUNTS'].apply(parse_counts)
        df_clean['NUM_COUNTS'] = df_clean['PARSED_COUNTS'].apply(len)
    
    # Parse THEMES field
    if 'THEMES' in df_clean.columns:
        print("  Parsing themes...")
        def parse_themes(themes_str):
            if pd.isna(themes_str):
                return []
            return [theme.strip() for theme in str(themes_str).split(';') if theme.strip()]
        
        df_clean['PARSED_THEMES'] = df_clean['THEMES'].apply(parse_themes)
        df_clean['NUM_THEMES'] = df_clean['PARSED_THEMES'].apply(len)
    
    # Parse LOCATIONS field
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
                            'lat': parts[4],
                            'lon': parts[5],
                            'feature_id': parts[6]
                        })
            return locations
        
        df_clean['PARSED_LOCATIONS'] = df_clean['LOCATIONS'].apply(parse_locations)
        df_clean['NUM_LOCATIONS'] = df_clean['PARSED_LOCATIONS'].apply(len)
    
    # Parse V2TONE field
    if 'V2TONE' in df_clean.columns:
        print("  Parsing tone analysis...")
        def parse_tone(tone_str):
            if pd.isna(tone_str):
                return {}
            parts = str(tone_str).split(',')
            if len(parts) >= 4:
                return {
                    'avg_tone': parts[0],
                    'positive_score': parts[1],
                    'negative_score': parts[2],
                    'polarity': parts[3] if len(parts) > 3 else None,
                    'activity_reference_density': parts[4] if len(parts) > 4 else None,
                    'self_group_reference_density': parts[5] if len(parts) > 5 else None
                }
            return {}
        
        df_clean['PARSED_TONE'] = df_clean['V2TONE'].apply(parse_tone)
    
    print(f"✅ Cleaning complete. Original shape: {df.shape}, Cleaned shape: {df_clean.shape}")
    
    return df_clean

def create_es_mapping_for_gkg():
    """Create Elasticsearch mapping for GKG data"""
    gkg_mapping = {
        "mappings": {
            "properties": {
                "GKGRECORDID": {"type": "keyword"},
                "DATE": {"type": "date"},
                "year": {"type": "integer"},
                "month": {"type": "integer"},
                "day": {"type": "integer"},
                "SOURCECOLLECTIONIDENTIFIER": {"type": "keyword"},
                "SOURCECOMMONNAME": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                },
                "DOCUMENTIDENTIFIER": {"type": "keyword"},
                "DOMAIN": {"type": "keyword"},
                "NUM_THEMES": {"type": "integer"},
                "NUM_LOCATIONS": {"type": "integer"},
                "NUM_COUNTS": {"type": "integer"},
                "THEMES": {"type": "text"},
                "PARSED_THEMES": {"type": "text"},
                "LOCATIONS": {"type": "text"},
                "PARSED_LOCATIONS": {
                    "type": "nested",
                    "properties": {
                        "type": {"type": "keyword"},
                        "full_name": {"type": "text"},
                        "country_code": {"type": "keyword"},
                        "adm1": {"type": "keyword"},
                        "lat": {"type": "float"},
                        "lon": {"type": "float"},
                        "feature_id": {"type": "keyword"}
                    }
                },
                "COUNTS": {"type": "text"},
                "PARSED_COUNTS": {
                    "type": "nested",
                    "properties": {
                        "type": {"type": "keyword"},
                        "count": {"type": "integer"},
                        "object": {"type": "text"}
                    }
                },
                "PERSONS": {"type": "text"},
                "ORGANIZATIONS": {"type": "text"},
                "V2TONE": {"type": "text"},
                "PARSED_TONE": {
                    "type": "object",
                    "properties": {
                        "avg_tone": {"type": "float"},
                        "positive_score": {"type": "float"},
                        "negative_score": {"type": "float"},
                        "polarity": {"type": "keyword"},
                        "activity_reference_density": {"type": "float"},
                        "self_group_reference_density": {"type": "float"}
                    }
                },
                "V2ENHANCEDDATES": {"type": "text"},
                "V2QUOTATIONS": {"type": "text"},
                "TIMESTAMP_INGESTED": {"type": "date"}
            }
        }
    }
    
    return gkg_mapping

def ingest_gkg_direct(es_client, df, index_name):
    """
    Ingests a cleaned GKG DataFrame directly into ES without an intermediate CSV.
    """
    # Convert DataFrame to a list of dictionaries
    # 'records' format preserves the nested structures (lists/dicts) created by your cleaning functions
    docs = df.to_dict('records')
    
    from elasticsearch.helpers import bulk
    
    def generate_actions():
        for doc in docs:
            # Handle the Time field safely
            if isinstance(doc.get('DATE'), pd.Timestamp):
                doc['Time'] = doc['DATE'].isoformat() + "Z"
            
            # Create a simple location field for map visualization if needed
            if 'PARSED_LOCATIONS' in doc and doc['PARSED_LOCATIONS']:
                first_loc = doc['PARSED_LOCATIONS'][0]
                doc['Location'] = f"{first_loc['lat']},{first_loc['lon']}"
            
            yield {
                "_index": index_name,
                "_source": doc
            }

    success, failed = bulk(es_client, generate_actions())
    print(f"✅ Successfully indexed {success} documents. Failed: {failed}")

def upload_gkg_to_elasticsearch(user, password, gkg_file_path, index_name="gkg_data"):
    df_raw = gkg_to_dataframe(gkg_file_path)
    df_cleaned = clean_gkg_data(df_raw) # This function is great, keep it!
    
    es = connect_to_es(user, password)
    
    # Use the rich GKG mapping you already wrote
    gkg_mapping = create_es_mapping_for_gkg()
    es.indices.create(index=index_name, body=gkg_mapping, ignore=400) # type: ignore
    
    # Step 5: Direct Ingest (No intermediate CSV!)
    ingest_gkg_direct(es, df_cleaned, index_name)

if __name__ == "__main__":
    mappings = read_json("mappings.json")
    countrycode_dict = read_json("country_code.json")
    load_dotenv()
    user = os.getenv("ESUSER")
    password = os.getenv("ESPASSWORD")
    upload_gkg_to_elasticsearch(
        user=user,
        password=password,
        gkg_file_path="C:\\Users\\ngyee\\Coding\\OTB\\AIS Data Retention\\20251110.gkg.csv",
        index_name="gkg_data"
    )