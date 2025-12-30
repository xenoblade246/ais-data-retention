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


def simple_setup_ilm(user, password, mappings, delete_after_days=14):
    """
    Simplified ILM setup that's more reliable.
    """
    try:
        es = connect_to_es(user, password)
        
        print("🔄 Setting up simplified ILM...")
        
        # 1. Create ILM policy
        with open("C:\\Users\\ngyee\\Coding\\OTB\\ais-data-retention\\country_code.json", "r") as f:
            policy_body = json.load(f)
        
        try:
            es.ilm.put_lifecycle(name="ais_policy", body=policy_body)
            print(f"✅ Created ILM policy - delete after {delete_after_days} days")
        except Exception as e:
            print(f"⚠️ Could not create ILM policy: {e}")
            print("ℹ️ Trying alternative approach...")
        
        # 2. Apply ILM to existing indices directly
        indices = es.indices.get(index="wstream_s*", ignore_unavailable=True)
        
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
        
        # 3. Create a new index with ILM for future data
        new_index_name = f"ais_data_{datetime.now().strftime('%Y%m%d')}"
        try:
            # Create index with mappings
            es.indices.create(
                index=new_index_name,
                body={
                    "settings": {
                        "index.lifecycle.name": "ais_policy",
                        "index.lifecycle.rollover_alias": "ais_current"
                    },
                    "mappings": mappings["mappings"]  # Use only mappings part
                }
            )
            
            # Set alias
            es.indices.update_aliases(body={
                "actions": [
                    {"add": {"index": new_index_name, "alias": "ais_current"}}
                ]
            })
            
            print(f"✅ Created new index for future data: {new_index_name}")
            
        except Exception as e:
            print(f"⚠️ Could not create new index: {e}")
        
        print("\n✅ Simplified ILM setup complete!")
        print(f"   • Existing indices: ILM applied for {delete_after_days}-day retention")
        print(f"   • Future data will use index: {new_index_name}")
        print(f"   • Old data will be automatically deleted after {delete_after_days} days")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in simplified ILM setup: {e}")
        return False


def setup_ilm_for_all_indices(user, password, delete_after_days=14):
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
        return simple_setup_ilm(user, password, delete_after_days)
        
    except Exception as e:
        print(f"❌ Error setting up ILM: {e}")
        return False


def check_ilm_status(user, password):
    """
    Check the status of ILM policies and indices.
    """
    try:
        es = connect_to_es(user, password)
        
        print("\n🔍 ILM Status Report")
        print("=" * 60)
        
        # Check if ILM is available
        try:
            # Try to get ILM status
            es.ilm.get_lifecycle()
            print("✅ ILM is available")
        except Exception as e:
            print(f"❌ ILM may not be available: {e}")
            print("ℹ️ ILM requires Elasticsearch X-Pack license (Basic or higher)")
            return
        
        # Check existing policies
        try:
            policies = es.ilm.get_lifecycle()
            if policies:
                print(f"\n📋 Found {len(policies)} ILM policies:")
                for policy_name in policies.keys():
                    print(f"  - {policy_name}")
            else:
                print("\n📋 No ILM policies found")
        except Exception as e:
            print(f"\n📋 Could not retrieve ILM policies: {e}")
        
        # Check indices
        print("\n📊 Index Status:")
        indices = es.indices.get(index="wstream_s*", ignore_unavailable=True)
        
        if not indices:
            print("  No indices found matching pattern 'wstream_s*'")
            return
        
        for index_name, index_info in indices.items():
            settings = index_info.get("settings", {}).get("index", {})
            ilm_settings = settings.get("lifecycle", {})
            
            print(f"\n  Index: {index_name}")
            
            if ilm_settings:
                print(f"\tILM Policy: {ilm_settings.get('name', 'Not set')}")
                print(f"\tRollover Alias: {ilm_settings.get('rollover_alias', 'Not set')}")
                
                # Get document count
                try:
                    stats = es.indices.stats(index=index_name)
                    docs = stats["indices"][index_name]["primaries"]["docs"]["count"]
                    print(f"\tDocument Count: {docs:,}")
                except Exception as e:
                    print(f"\tDocument Count: Unknown\n\tError: {e}")
                    
                # Check for old data
                try:
                    from datetime import datetime, timedelta
                    cutoff = datetime.utcnow() - timedelta(days=14)
                    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    
                    count_query = {
                        "query": {
                            "range": {
                                "Time": {"lt": cutoff_str}
                            }
                        }
                    }
                    
                    count_response = es.count(index=index_name, body=count_query)
                    old_count = count_response.get("count", 0)
                    print(f"\tDocuments older than 14 days: {old_count:,}")
                except Exception as e:
                    print(f"\tOld document count: Could not check\n\tError: {e}")
            else:
                print("\tILM: Not configured")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"❌ Error checking ILM status: {e}")


def manual_delete_old_data(user, password, days_old=14):
    """
    Manually delete data older than specified number of days.
    This can be used as a backup if ILM doesn't work properly.
    
    Args:
        user: Elasticsearch username
        password: Elasticsearch password
        days_old: Delete data older than this many days
    """
    try:
        es = connect_to_es(user, password)
        
        # Calculate cutoff time (now minus X days)
        cutoff_time = tm.time() - (days_old * 24 * 3600)
        cutoff_date_str = epoch_to_estime(int(cutoff_time))
        
        # Create delete query
        delete_query = {
            "query": {
                "range": {
                    "Time": {
                        "lt": cutoff_date_str
                    }
                }
            }
        }
        
        # Get all indices matching the pattern
        indices = es.indices.get(index="wstream_s*")
        
        total_deleted = 0
        for index_name in indices.keys():
            try:
                # Execute delete by query
                response = es.delete_by_query(
                    index=index_name,
                    body=delete_query,
                    conflicts="proceed",
                    refresh=True
                )
                
                deleted_count = response.get("deleted", 0)
                total_deleted += deleted_count
                print(f"✅ Deleted {deleted_count} documents from {index_name}")
                
            except Exception as e:
                print(f"⚠️ Could not delete from {index_name}: {e}")
        
        print(f"\n📊 Total documents deleted: {total_deleted}")
        return total_deleted
        
    except Exception as e:
        print(f"❌ Error during manual deletion: {e}")
        return 0


def create_daily_cleanup_task(user, password):
    """
    Create a function that can be scheduled to run daily cleanup.
    This ensures data older than 2 weeks is deleted even if ILM fails.
    """
    print("🔄 Starting daily cleanup task...")
    
    # 1. First, try to use manual deletion as backup
    deleted_count = manual_delete_old_data(user, password, days_old=14)
    
    # 2. Force ILM to run if needed
    es = connect_to_es(user, password)
    es.ilm.retry_policy(index="wstream_s*") #type: ignore
    print("✅ Triggered ILM policy retry")
    
    # 3. Check ILM status
    check_ilm_status(user, password)
    
    print(f"✅ Daily cleanup completed. Deleted {deleted_count} old documents.")
    return deleted_count


def epoch_to_estime(epoch: int) -> str:
    """
    Convert epoch time to a formatted string in UTC, adjusted for the timezone differe  nce.

    Args:
        epoch (int): The epoch time to convert.

    Returns:
        str: The formatted time string in "YYYY-MM-DDTHH:MM:SS.000Z" format.
    """
    epoch -= 8 * 3600
    return tm.strftime("%Y-%m-%dT%H:%M:%S.000Z", tm.localtime(epoch))

def create_index_template(es, mappings, template_name="ais_data_template", index_pattern="wstream_s*", 
                          policy_name="ais_data_policy"):
    """
    Create an index template that applies the ILM policy to matching indices.
    
    Args:
        es: Elasticsearch client
        template_name: Name of the index template
        index_pattern: Pattern for indices to apply the template to
        policy_name: Name of the ILM policy to apply
    """
    template = {
        "index_patterns": [index_pattern],
        "template": {
            "settings": {
                "index.lifecycle.name": policy_name,
                "index.lifecycle.rollover_alias": "ais_data",
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": mappings  # Using your existing mappings
        },
        "priority": 100
    }
    
    try:
        response = es.indices.put_index_template(name=template_name, body=template)  # FIXED: Added name= keyword
        print(f"✅ Created index template '{template_name}' for pattern '{index_pattern}'")
        return response
    except Exception as e:
        print(f"❌ Error creating index template: {e}")
        return None

def create_ilm_policy(es: Elasticsearch, policy_name="ais_data_policy", delete_after_days=14):
    """
    Create or update an Index Lifecycle Management (ILM) policy for automatic deletion.
    
    Args:
        es: Elasticsearch client
        policy_name: Name of the ILM policy
        delete_after_days: Number of days to keep data before deletion
    """
    policy = {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "1d",
                            "max_size": "50gb",
                            "max_docs": 1000000
                        },
                        "set_priority": {
                            "priority": 100
                        }
                    }
                },
                "delete": {
                    "min_age": f"{delete_after_days}d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    }
    
    try:
        response = es.ilm.put_lifecycle(name=policy_name, body=policy)  # FIXED: Added name= keyword
        print(f"✅ Created/updated ILM policy '{policy_name}' - data will be deleted after {delete_after_days} days")
        return response
    except Exception as e:
        print(f"❌ Error creating ILM policy: {e}")
        return None

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
    print("Automatically delete data older than 2 weeks")
    print("="*60)
    
    while True:
        print("\n" + "="*60)
        print("ILM MANAGEMENT MENU")
        print("="*60)
        print("1. Setup ILM Policy (Delete after 14 days)")
        print("2. Check ILM Status")
        print("3. Manual Cleanup (Delete old data now)")
        print("4. Run Daily Cleanup Task")
        print("5. Setup with Custom Retention (specify days)")
        print("6. Exit")
        print("="*60)
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            print("\n🔄 Setting up ILM policy...")
            setup_ilm_for_all_indices(user, password, 14)
            
        elif choice == '2':
            print("\n🔍 Checking ILM status...")
            check_ilm_status(user, password)
            
        elif choice == '3':
            print("\n🗑️ Running manual cleanup...")
            confirm = input("Delete all data older than 14 days? (yes/no): ").strip().lower()
            if confirm == 'yes':
                deleted = manual_delete_old_data(user, password, 14)
                print(f"✅ Deleted {deleted} documents")
            else:
                print("❌ Cleanup cancelled")
                
        elif choice == '4':
            print("\n🔄 Running daily cleanup task...")
            create_daily_cleanup_task(user, password)
            
        elif choice == '5':
            try:
                days = int(input("Enter retention period in days: ").strip())
                print(f"\n🔄 Setting up ILM with {days}-day retention...")
                setup_ilm_for_all_indices(user, password, days)
            except ValueError:
                print("❌ Please enter a valid number")
                
        elif choice == '6':
            print("\n👋 Exiting ILM Manager. Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    user, password = setup_environment()
    main_menu(user, password)