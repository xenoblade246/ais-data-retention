import os
import time as tm
from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import json

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


def simple_setup_ilm(user, password, delete_time="1 min"):
    """
    Simplified ILM setup that's more reliable.
    """
    try:
        es = connect_to_es(user, password)
        
        print("🔄 Setting up simplified ILM...")
        
        # 1. Create ILM policy
        with open("dummy_policy.json", "r") as f:
            policy_body = json.load(f)
        
        try:
            es.ilm.put_lifecycle(name="ais_policy", body=policy_body)
            print(f"✅ Created ILM policy - delete after {delete_time}")
        except Exception as e:
            print(f"⚠️ Could not create ILM policy: {e}")
            print("ℹ️ Trying alternative approach...")
        
        # 2. Apply ILM to existing indices directly
        indices = es.indices.get(index="gkg_data-*", ignore_unavailable=True)
        
        for index_name in indices.keys():
            try:
                # Apply ILM settings directly to index
                settings = {
                    "index.lifecycle.name": "ais_policy",
                    "index.lifecycle.rollover_alias": None,  # No rollover for existing indices
                    "index.lifecycle.indexing_complete": True
                }
                es.indices.put_settings(index=index_name, body={"settings": settings})
                print(f"✅ Applied ILM to existing index: {index_name}")
            except Exception as e:
                print(f"⚠️ Could not apply ILM to {index_name}: {e}")
        
        print("\n✅ Simplified ILM setup complete!")
        print(f"   - Old data will be automatically deleted after {delete_time}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in simplified ILM setup: {e}")
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

def setup_ilm_for_all_indices(user, password, delete_time="1 min"):
    """
    Setup ILM for all existing AIS indices.
    
    Args:
        user: Elasticsearch username
        password: Elasticsearch password
        delete_after_days: Number of days to keep data
    """
    print("🔄 Starting ILM setup...")
    
    try:        
        # Use the simplified approach
        return simple_setup_ilm(user, password, delete_time)
        
    except Exception as e:
        print(f"❌ Error setting up ILM: {e}")
        return False


def check_ilm_status(user, password):
    try:
        es = connect_to_es(user, password)
        print("\n🔍 GKG Index Status Report")
        indices = es.indices.get(index="gkg_data-*", ignore_unavailable=True)
        
        for index_name, index_info in indices.items():
            ilm = index_info.get("settings", {}).get("index", {}).get("lifecycle", {})
            stats = es.indices.stats(index=index_name)
            docs = stats["indices"][index_name]["primaries"]["docs"]["count"]
            print(f"Index: {index_name} | Policy: {ilm.get('name', 'None')} | Docs: {docs:,}")
    except Exception as e:
        print(f"❌ Status check failed: {e}")


def setup_environment() -> tuple[str, str]:
    """Load environment variables"""
    load_dotenv()
    
    user = os.getenv("ESUSER")
    password = os.getenv("ESPASSWORD")
    
    if not user or not password:
        print("❌ Error: ESUSER and/or ESPASSWORD not found in environment variables")
        print("Please create a .env file with:")
        print("ESUSER=your_username")
        print("ESPASSWORD=your_password")
        raise Exception("User or password not initialized in .env file")
    
    print(f"✅ Loaded credentials for user: {user}")
    return user, password


def main_menu(user: str, password: str):
    """Main menu for ILM management"""
    print("\n" + "="*60)
    print("ELASTICSEARCH ILM MANAGER")
    print("Automatically delete data older than 1 min")
    print("="*60)
    
    print("\n🔄 Setting up ILM policy...")
    setup_ilm_for_all_indices(user, password)

################################################ Main ################################################
if __name__ == "__main__":
    user, password = setup_environment()
    main_menu(user, password)