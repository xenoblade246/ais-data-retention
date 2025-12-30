import os
import pandas as pd
from datetime import datetime, timezone
import json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import time as tm
from typing import Optional

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

def ingestpastdata(user: Optional[str], password: Optional[str], indexname: str, mappings: dict, file: str):
    """
    Ingests data from a CSV file into an Elasticsearch index.

    This function performs an ETL (Extract, Transform, Load) process:
    1. Connects to the Elasticsearch instance.
    2. Creates the specified index with provided mappings if it doesn't exist.
    3. Processes the CSV file to ensure specific column ordering and handles missing values.
    4. Transforms Unix epoch timestamps into ISO 8601 UTC strings.
    5. Formats latitude and longitude into a 'Location' list for geo-spatial indexing.
    6. Loads the data row-by-row into the database.

    Args:
        user (Optional[str]): Username for Elasticsearch authentication.
        password (Optional[str]): Password for Elasticsearch authentication.
        indexname (str): The name of the target index in Elasticsearch.
        mappings (dict): A dictionary defining the Elasticsearch index settings and schema.
        countrycodeDict (dict): A dictionary used to look up country names via MMSI prefixes.
        file (str): The file path to the source CSV data.

    Raises:
        Exception: If a connection error occurs or if document indexing fails.
    """
    try:
        # Connect to Elastic Search DB
        es = connect_to_es(user, password)
        es.indices.create(index=indexname, body=mappings, ignore=400) #type: ignore
        df = pd.read_csv(file)
        # Define the expected columns in sequence
        expectedCol = [
            "Country",
            "MMSI",
            "Lat",
            "Lon",
            "Name",
            "Time",
            "Source",
            "Subsource",
            "Speed",
            "Course",
        ]
        # Rearrange the dataframe
        df = df[expectedCol].copy()

        # Convert epoch time in 'Time' column to ISO format
        def convert_epoch_to_iso(epoch_time):
            try:
                if isinstance(epoch_time, (int, float)):
                    dt_object = datetime.fromtimestamp(epoch_time, tz=timezone.utc)
                elif isinstance(epoch_time, str) and epoch_time.isdigit():
                    dt_object = datetime.fromtimestamp(int(epoch_time), tz=timezone.utc)
                else:
                    return ""
                return dt_object.isoformat(timespec='microseconds').replace('+00:00', 'Z')
            except (ValueError, TypeError):
                return ""

        df['Time'] = df['Time'].apply(convert_epoch_to_iso)

        # Account for missing values
        df = df.fillna("")
        df_dict = df.to_dict("records")

        # Iterate through each row of the dictionary
        for i in range(len(df_dict)):
            report = {}
            location = [str(df_dict[i]["Lat"]) + "," + str(df_dict[i]["Lon"])]

            # Assign Key value pairs
            for key in expectedCol:
                report[key] = df_dict[i][key]
            report["Location"] = location
            doc = json.dumps(report)

            # Feeds into Elasticsearch
            try:
                es.index(index=indexname, body=doc) #type: ignore
            except Exception as e:
                raise Exception(e)

        print("Success", "Data ingestion completed successfully.")

    except Exception as e:
        print("Error", f"An unexpected error occurred: {str(e)}")

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
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                domain = parsed.netloc
                # Remove www. prefix
                if domain.startswith('www.'):
                    domain = domain[4:]
                return domain
            except:
                return None
        
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

def convert_to_csv_for_es(df, output_csv_path):
    """
    Convert GKG DataFrame to CSV format compatible with Elasticsearch ingestion
    
    Args:
        df: GKG DataFrame
        output_csv_path: Path to save CSV file
    """
    print(f"💾 Converting to CSV: {output_csv_path}")
    
    # Select and rename columns for Elasticsearch compatibility
    es_df = pd.DataFrame()
    
    # Map GKG columns to AIS-like structure where possible
    if 'PARSED_LOCATIONS' in df.columns:
        # Extract first location for geo mapping
        def get_first_location(locations):
            if locations and len(locations) > 0:
                return locations[0]
            return None
        
        first_locs = df['PARSED_LOCATIONS'].apply(get_first_location)
        
        es_df['Country'] = first_locs.apply(lambda x: x.get('full_name') if x else '')
        es_df['Lat'] = first_locs.apply(lambda x: float(x.get('lat')) if x and x.get('lat') else None)
        es_df['Lon'] = first_locs.apply(lambda x: float(x.get('lon')) if x and x.get('lon') else None)
    
    # Add GKG metadata
    es_df['MMSI'] = df['GKGRECORDID']  # Using GKGRECORDID as identifier
    es_df['Name'] = df['SOURCECOMMONNAME'].fillna('Unknown Source')
    es_df['Time'] = df['DATE'].apply(lambda x: x.isoformat() if pd.notna(x) else '')
    
    # Source information
    es_df['Source'] = 'GKG'
    es_df['Subsource'] = df['SOURCECOLLECTIONIDENTIFIER'].apply(
        lambda x: {
            '1': 'Web',
            '2': 'Citation',
            '3': 'Core',
            '4': 'DTIC',
            '5': 'JSTOR',
            '6': 'USGS'
        }.get(str(x), 'Unknown')
    )
    
    # Additional GKG metadata
    es_df['Speed'] = df['NUM_THEMES'].fillna(0)  # Using number of themes as "speed"
    es_df['Course'] = df['PARSED_TONE'].apply(
        lambda x: x.get('avg_tone', '0') if isinstance(x, dict) else '0'
    )
    
    # Add raw GKG fields for reference
    es_df['GKG_THEMES'] = df['THEMES'].fillna('')
    es_df['GKG_LOCATIONS'] = df['LOCATIONS'].fillna('')
    es_df['GKG_ORGANIZATIONS'] = df['ORGANIZATIONS'].fillna('')
    es_df['GKG_PERSONS'] = df['PERSONS'].fillna('')
    es_df['GKG_TONE_AVG'] = df['PARSED_TONE'].apply(
        lambda x: x.get('avg_tone', '') if isinstance(x, dict) else ''
    )
    
    # Save to CSV
    es_df.to_csv(output_csv_path, index=False)
    print(f"✅ CSV saved with {len(es_df)} rows, {len(es_df.columns)} columns")
    
    return output_csv_path

def upload_gkg_to_elasticsearch(user, password, gkg_file_path, index_name="gkg_data", 
                                sample_size=None, clean_data=True):
    """
    Main function to upload GKG file to Elasticsearch
    
    Args:
        user: Elasticsearch username
        password: Elasticsearch password
        gkg_file_path: Path to GKG file
        index_name: Elasticsearch index name
        sample_size: Number of rows to sample (for testing)
        clean_data: Whether to clean data before upload
    """
    print("\n🚀 Starting GKG upload process")
    print(f"   File: {gkg_file_path}")
    print(f"   Index: {index_name}")
    
    # Step 1: Load GKG data
    df_raw = gkg_to_dataframe(gkg_file_path, sample_size) # type: ignore
    if df_raw is None:
        return False
    
    # Step 2: Clean data (optional)
    if clean_data:
        df = clean_gkg_data(df_raw)
    else:
        df = df_raw
    
    # Step 3: Create temporary CSV
    temp_csv = f"temp_gkg_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_path = convert_to_csv_for_es(df, temp_csv)
    
    # Step 4: Connect to Elasticsearch
    print("\n🔗 Connecting to Elasticsearch...")
    try:
        es = connect_to_es(user, password)
        
        # Create index with GKG-specific mapping
        gkg_mapping = create_es_mapping_for_gkg()
        es.indices.create(index=index_name, body=gkg_mapping, ignore=400) # type: ignore
        print(f"✅ Created/verified index: {index_name}")
        
        # Step 5: Upload using existing ingest function
        print("\n📤 Uploading to Elasticsearch...")
        ingestpastdata(
            user=user,
            password=password,
            indexname=index_name,
            mappings=gkg_mapping,
            file=csv_path
        )
        
        # Step 6: Verify upload
        print("\n✅ Upload complete! Verifying...")
        result = es.count(index=index_name)
        print(f"   Documents in index: {result['count']:,}")
        
        # Step 7: Clean up temporary file
        os.remove(csv_path)
        print(f"   Cleaned up temporary file: {csv_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during upload: {e}")
        import traceback
        traceback.print_exc()
        return False

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