# gkg_uploader.py

import os
import pandas as pd
import numpy as np
from datetime import datetime
import csv
import json
from dotenv import load_dotenv

# Import from your existing aisUtil module
from aisUtil import (
    connect_to_es,
    wait_for_elasticsearch
)

def read_json(name: str) -> dict:
    """
    Reads the JSON file contents, & outputs the result as a dict.

    Returns:
        dict: The JSON file content.
    """
    with open(name, "r") as f:
        out = json.load(f)
    return out

def ingestpastdata(user: Optional[str], password: Optional[str], indexname: str, mappings: dict, countrycodeDict: dict, file: str) -> None:
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
            location = [
                str(df_dict[i]["Lat"]) + "," + str(df_dict[i]["Lon"])
            ]
            country = countrycodeDict.get(str(df_dict[i]["MMSI"])[:3], [None, None])[0]
            # Assign Key value pairs
            (
                report["Lat"],
                report["Lon"],
                report["MMSI"],
                report["Name"],
                report["Location"],
                report["Time"],
                report["Country"],
                report["Source"],
                report["Subsource"],
                report["Speed"],
                report["Course"],
            ) = (
                df_dict[i]["Lat"],
                df_dict[i]["Lon"],
                df_dict[i]["MMSI"],
                df_dict[i]["Name"],
                location,
                df_dict[i]["Time"],
                df_dict[i]["Country"],
                df_dict[i]["Source"],
                df_dict[i]["Subsource"],
                df_dict[i]["Speed"],
                df_dict[i]["Course"],
            )
            doc = json.dumps(report)
            try:
                es.index(index=indexname, body=doc) #type: ignore
                print(doc)
            except Exception as e:
                raise Exception(e)

        messagebox.showinfo("Success", "Data ingestion completed successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"An unexpected error occurred: {str(e)}")

def setup_environment():
    """Load environment variables"""
    load_dotenv()
    
    user = os.getenv("ESUSER")
    password = os.getenv("ESPASSWORD")
    
    if not user or not password:
        print("❌ Error: ESUSER and/or ESPASSWORD not found in environment variables")
        print("Please create a .env file with:")
        print("ESUSER=your_username")
        print("ESPASSWORD=your_password")
        return None, None
    
    print(f"✅ Loaded credentials for user: {user}")
    return user, password

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
    print(f"\n🚀 Starting GKG upload process")
    print(f"   File: {gkg_file_path}")
    print(f"   Index: {index_name}")
    
    # Step 1: Load GKG data
    df_raw = gkg_to_dataframe(gkg_file_path, sample_size)
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
        es.indices.create(index=index_name, body=gkg_mapping, ignore=400)
        print(f"✅ Created/verified index: {index_name}")
        
        # Step 5: Upload using existing ingest function
        print("\n📤 Uploading to Elasticsearch...")
        ingestpastdata(
            user=user,
            password=password,
            indexname=index_name,
            mappings=gkg_mapping,
            countrycodeDict={},  # Not using country codes for GKG
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

def gkg_explorer(gkg_file_path, num_rows=5):
    """Explore GKG file structure and content"""
    print(f"\n🔍 Exploring GKG file: {gkg_file_path}")
    
    df = gkg_to_dataframe(gkg_file_path, sample_size=num_rows)
    if df is None:
        return
    
    print(f"\n📋 Sample data (first {num_rows} rows):")
    print("-" * 80)
    
    # Display key columns
    display_cols = ['GKGRECORDID', 'DATE', 'SOURCECOMMONNAME', 'NUM_THEMES', 'NUM_LOCATIONS']
    display_cols = [col for col in display_cols if col in df.columns]
    
    print(df[display_cols].head(num_rows).to_string())
    
    # Show column statistics
    print(f"\n📊 Column statistics:")
    print("-" * 80)
    for col in df.columns:
        non_null = df[col].notna().sum()
        null = df[col].isna().sum()
        unique = df[col].nunique()
        print(f"{col:30s} | Non-null: {non_null:6d} | Null: {null:6d} | Unique: {unique:6d}")
    
    # Show THEMES examples
    if 'THEMES' in df.columns:
        print(f"\n🎯 Sample themes:")
        themes_sample = df['THEMES'].dropna().head(3).tolist()
        for i, themes in enumerate(themes_sample[:3]):
            print(f"  {i+1}. {themes[:100]}...")
    
    # Show LOCATIONS examples
    if 'LOCATIONS' in df.columns:
        print(f"\n📍 Sample locations:")
        locs_sample = df['LOCATIONS'].dropna().head(3).tolist()
        for i, locs in enumerate(locs_sample[:3]):
            print(f"  {i+1}. {locs[:100]}...")

def main():
    """Main menu for GKG uploader"""
    print("\n" + "="*60)
    print("GKG (Global Knowledge Graph) UPLOADER TO ELASTICSEARCH")
    print("="*60)
    
    # Setup environment
    user, password = setup_environment()
    if not user or not password:
        return
    
    while True:
        print("\n" + "="*60)
        print("GKG UPLOADER MENU")
        print("="*60)
        print("1. Explore GKG file structure")
        print("2. Upload GKG file to Elasticsearch")
        print("3. Convert GKG to CSV (without upload)")
        print("4. Clean and preview GKG data")
        print("5. Exit")
        print("="*60)
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            gkg_file = input("Enter GKG file path: ").strip()
            if os.path.exists(gkg_file):
                sample_size = input("Number of rows to sample (default 5): ").strip()
                sample_size = int(sample_size) if sample_size.isdigit() else 5
                gkg_explorer(gkg_file, sample_size)
            else:
                print(f"❌ File not found: {gkg_file}")
        
        elif choice == '2':
            gkg_file = input("Enter GKG file path: ").strip()
            if os.path.exists(gkg_file):
                index_name = input("Enter index name (default: gkg_data): ").strip()
                if not index_name:
                    index_name = "gkg_data"
                
                sample_option = input("Sample data? (y/n, default n): ").strip().lower()
                if sample_option == 'y':
                    sample_size = input("Number of rows to upload: ").strip()
                    sample_size = int(sample_size) if sample_size.isdigit() else 1000
                else:
                    sample_size = None
                
                clean_option = input("Clean data before upload? (y/n, default y): ").strip().lower()
                clean_data = clean_option != 'n'
                
                upload_gkg_to_elasticsearch(
                    user=user,
                    password=password,
                    gkg_file_path=gkg_file,
                    index_name=index_name,
                    sample_size=sample_size,
                    clean_data=clean_data
                )
            else:
                print(f"❌ File not found: {gkg_file}")
        
        elif choice == '3':
            gkg_file = input("Enter GKG file path: ").strip()
            if os.path.exists(gkg_file):
                output_csv = input("Enter output CSV path (default: gkg_output.csv): ").strip()
                if not output_csv:
                    output_csv = "gkg_output.csv"
                
                sample_size = input("Number of rows to convert (default all): ").strip()
                sample_size = int(sample_size) if sample_size.isdigit() else None
                
                df = gkg_to_dataframe(gkg_file, sample_size)
                if df is not None:
                    df_clean = clean_gkg_data(df)
                    convert_to_csv_for_es(df_clean, output_csv)
            else:
                print(f"❌ File not found: {gkg_file}")
        
        elif choice == '4':
            gkg_file = input("Enter GKG file path: ").strip()
            if os.path.exists(gkg_file):
                sample_size = input("Number of rows to preview (default 100): ").strip()
                sample_size = int(sample_size) if sample_size.isdigit() else 100
                
                df_raw = gkg_to_dataframe(gkg_file, sample_size)
                if df_raw is not None:
                    df_clean = clean_gkg_data(df_raw)
                    
                    print(f"\n🧹 Cleaning statistics:")
                    print(f"   Original rows: {len(df_raw)}")
                    print(f"   Cleaned rows: {len(df_clean)}")
                    print(f"   New columns added: {len(df_clean.columns) - len(df_raw.columns)}")
                    
                    print(f"\n📊 First few rows of cleaned data:")
                    print(df_clean.head(3).to_string())
            else:
                print(f"❌ File not found: {gkg_file}")
        
        elif choice == '5':
            print("\n👋 Exiting GKG Uploader. Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    mappings = read_json("mappings.json")
    countrycode_dict = read_json("country_code.json")
    # Command line support
    import sys
    
    if len(sys.argv) > 1:
        # Quick upload from command line
        if sys.argv[1] == "upload" and len(sys.argv) >= 3:
            gkg_file = sys.argv[2]
            index_name = sys.argv[3] if len(sys.argv) >= 4 else "gkg_data"
            
            user, password = setup_environment()
            if user and password:
                upload_gkg_to_elasticsearch(
                    user=user,
                    password=password,
                    gkg_file_path=gkg_file,
                    index_name=index_name
                )
        elif sys.argv[1] == "explore" and len(sys.argv) >= 3:
            gkg_file = sys.argv[2]
            sample_size = int(sys.argv[3]) if len(sys.argv) >= 4 else 5
            gkg_explorer(gkg_file, sample_size)
        else:
            print("Usage:")
            print("  python gkg_uploader.py upload <gkg_file> [index_name]")
            print("  python gkg_uploader.py explore <gkg_file> [sample_size]")
    else:
        # Interactive mode
        main()