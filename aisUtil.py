import time as tm
import pandas as pd
from elasticsearch import Elasticsearch
import tkinter as tk
from elasticsearch.exceptions import NotFoundError
import json
import csv
import websockets
import asyncio
import os
import pyais
import datetime as dt
from tkinter import messagebox, filedialog
from datetime import datetime
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
from dotenv import load_dotenv
from typing import Optional

load_dotenv()
user = os.getenv("ESUSER")
password = os.getenv("ESPASSWORD")

############################################################################ Functions ############################################################################
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



def create_initial_rollover_index(es):
    """
    Create the initial index for rollover.
    """
    index_name = "wstream_s-000001"
    
    try:
        # Check if index already exists
        if not es.indices.exists(index=index_name):
            # Create index with ILM settings - FIXED: Proper structure
            create_index_body = {
                "settings": {
                    "index.lifecycle.name": "ais_data_policy",
                    "index.lifecycle.rollover_alias": "ais_data",
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": mappings["mappings"]  # FIXED: Only use the "mappings" part
            }
            
            es.indices.create(index=index_name, body=create_index_body)
            
            # Add alias - FIXED: Proper alias structure
            alias_body = {
                "actions": [
                    {
                        "add": {
                            "index": index_name,
                            "alias": "ais_data",
                            "is_write_index": True  # Important for rollover
                        }
                    }
                ]
            }
            es.indices.update_aliases(body=alias_body)
            
            print(f"✅ Created initial rollover index: {index_name}")
        else:
            print(f"ℹ️ Initial rollover index already exists: {index_name}")
            
    except Exception as e:
        print(f"❌ Error creating initial rollover index: {e}")

# SIMPLER ALTERNATIVE - Use this if the above is still problematic

def simple_setup_ilm(user, password, delete_after_days=14):
    """
    Simplified ILM setup that's more reliable.
    """
    try:
        es = connect_to_es(user, password)
        
        print("🔄 Setting up simplified ILM...")
        
        # 1. Create ILM policy
        policy_body = {
            "policy": {
                "phases": {
                    "hot": {
                        "actions": {
                            "rollover": {
                                "max_age": "1d",
                                "max_primary_shard_size": "50gb"
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

def apply_ilm_to_index(es, index_name, policy_name="ais_policy"):
    """
    Apply ILM policy to a specific index.
    """
    try:
        # First, try to update settings
        settings_body = {
            "index.lifecycle.name": policy_name,
            "index.lifecycle.indexing_complete": True
        }
        
        es.indices.put_settings(index=index_name, body={"settings": settings_body})
        print(f"✅ Applied ILM policy to {index_name}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to apply ILM to {index_name}: {e}")
        return False

def delete_old_data_simple(user, password, days_old=14):
    """
    Simple manual deletion of old data.
    """
    try:
        es = connect_to_es(user, password)
        
        # Calculate cutoff
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        print(f"🗑️ Deleting data older than {cutoff_str}...")
        
        delete_query = {
            "query": {
                "range": {
                    "Time": {
                        "lt": cutoff_str
                    }
                }
            }
        }
        
        total_deleted = 0
        
        # Get all indices
        indices = es.indices.get(index="wstream_s*", ignore_unavailable=True)
        
        for index_name in indices.keys():
            try:
                # Count old documents first
                count_response = es.count(index=index_name, body=delete_query)
                old_count = count_response.get("count", 0)
                
                if old_count > 0:
                    # Delete old documents
                    delete_response = es.delete_by_query(
                        index=index_name,
                        body=delete_query,
                        conflicts="proceed",
                        refresh=True
                    )
                    
                    deleted = delete_response.get("deleted", 0)
                    total_deleted += deleted
                    print(f"  {index_name}: Deleted {deleted}/{old_count} old documents")
                else:
                    print(f"  {index_name}: No old documents found")
                    
            except Exception as e:
                print(f"  {index_name}: Error - {e}")
        
        print(f"\n✅ Total deleted: {total_deleted} documents")
        return total_deleted
        
    except Exception as e:
        print(f"❌ Error in manual deletion: {e}")
        return 0

# Updated setup_ilm_for_all_indices function
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
        es = connect_to_es(user, password)
        
        # Use the simplified approach
        return simple_setup_ilm(user, password, delete_after_days)
        
    except Exception as e:
        print(f"❌ Error setting up ILM: {e}")
        return False

# Updated check_ilm_status function
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
        except:
            print("\n📋 Could not retrieve ILM policies")
        
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
                print(f"    ILM Policy: {ilm_settings.get('name', 'Not set')}")
                print(f"    Rollover Alias: {ilm_settings.get('rollover_alias', 'Not set')}")
                
                # Get document count
                try:
                    stats = es.indices.stats(index=index_name)
                    docs = stats["indices"][index_name]["primaries"]["docs"]["count"]
                    print(f"    Document Count: {docs:,}")
                except:
                    print(f"    Document Count: Unknown")
                    
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
                    print(f"    Documents older than 14 days: {old_count:,}")
                except:
                    print(f"    Old document count: Could not check")
            else:
                print(f"    ILM: Not configured")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"❌ Error checking ILM status: {e}")

def apply_ilm_to_existing_indices(es, policy_name="ais_data_policy"):
    """
    Apply ILM settings to all existing wstream_s* indices.
    
    Args:
        es: Elasticsearch client
        policy_name: Name of the ILM policy
    """
    try:
        # Get all indices matching the pattern
        indices = es.indices.get(index="wstream_s*")
        
        for index_name in indices.keys():
            try:
                # Apply ILM policy to existing index
                settings = {
                    "index.lifecycle.name": policy_name
                }
                es.indices.put_settings(index=index_name, body=settings)
                print(f"✅ Applied ILM policy to existing index: {index_name}")
                
                # Add alias for rollover if not present
                aliases = es.indices.get_alias(index=index_name)
                if "ais_data" not in aliases.get(index_name, {}).get("aliases", {}):
                    alias_body = {
                        "actions": [
                            {
                                "add": {
                                    "index": index_name,
                                    "alias": "ais_data"
                                }
                            }
                        ]
                    }
                    es.indices.update_aliases(body=alias_body)
                    print(f"✅ Added rollover alias to: {index_name}")
                    
            except Exception as e:
                print(f"⚠️ Could not apply ILM to {index_name}: {e}")
                
    except NotFoundError:
        print("ℹ️ No existing indices found matching pattern 'wstream_s*'")
    except Exception as e:
        print(f"❌ Error applying ILM to existing indices: {e}")

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
    try:
        es = connect_to_es(user, password)
        es.ilm.retry_policy(index="wstream_s*") #type: ignore
        print("✅ Triggered ILM policy retry")
    except:
        pass
    
    # 3. Check ILM status
    check_ilm_status(user, password)
    
    print(f"✅ Daily cleanup completed. Deleted {deleted_count} old documents.")
    return deleted_count

# Add a GUI function for ILM management
def manage_ilm_gui():
    """
    GUI function to manage ILM policies.
    """
    import tkinter as tk
    from tkinter import ttk, messagebox
    
    user = os.getenv("ESUSER")
    password = os.getenv("ESPASSWORD")
    
    def setup_ilm():
        if messagebox.askyesno("Confirm", "This will setup ILM to automatically delete data older than 2 weeks. Continue?"):
            success = setup_ilm_for_all_indices(user, password, 14)
            if success:
                messagebox.showinfo("Success", "ILM setup completed successfully!")
            else:
                messagebox.showerror("Error", "Failed to setup ILM.")
    
    def check_status():
        check_ilm_status(user, password)
        messagebox.showinfo("Status", "ILM status has been printed to console.")
    
    def manual_cleanup():
        if messagebox.askyesno("Confirm", "Manually delete all data older than 2 weeks?"):
            deleted = manual_delete_old_data(user, password, 14)
            messagebox.showinfo("Complete", f"Deleted {deleted} documents.")
    
    def run_daily_cleanup():
        if messagebox.askyesno("Confirm", "Run daily cleanup task?"):
            deleted = create_daily_cleanup_task(user, password)
            messagebox.showinfo("Complete", f"Daily cleanup completed. Deleted {deleted} documents.")
    
    # Create GUI
    root = tk.Tk()
    root.title("ILM Policy Management")
    root.geometry("400x300")
    
    # Title
    title_label = tk.Label(root, text="Index Lifecycle Management", font=("Arial", 16, "bold"))
    title_label.pack(pady=10)
    
    # Description
    desc_label = tk.Label(root, text="Automatically delete data older than 2 weeks", wraplength=350)
    desc_label.pack(pady=5)
    
    # Buttons
    button_frame = tk.Frame(root)
    button_frame.pack(pady=20)
    
    buttons = [
        ("Setup ILM Policy", setup_ilm),
        ("Check ILM Status", check_status),
        ("Manual Cleanup", manual_cleanup),
        ("Run Daily Cleanup", run_daily_cleanup),
        ("Exit", root.quit)
    ]
    
    for text, command in buttons:
        btn = tk.Button(button_frame, text=text, command=command, width=20)
        btn.pack(pady=5)
    
    root.mainloop()

# Add this to your existing printMessage function section
def ilm_menu_option():
    """
    Add ILM management as an option in your existing menu system.
    """
    manage_ilm_gui()

def convert_to_epoch(date_string: str) -> int:
    """
    Convert a date string to epoch time, adjusting for timezone difference.

    Args:
        date_string (str): The date string in "%Y-%m-%dT%H:%M:%S.%f" or "%Y-%m-%dT%H:%M:%S" format.

    Returns:
        int: The corresponding epoch time.
    """
    try:
        # Try parsing with milliseconds
        date_obj = dt.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        # Fall back to parsing without milliseconds
        date_obj = dt.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    # Adjust time by adding 8 hours
    date_obj += dt.timedelta(hours=8)

    # Convert to epoch time
    return int(date_obj.timestamp())


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


def wait_for_elasticsearch(user, password, max_retries=30, delay=5):
    """
    Wait for Elasticsearch to be available before proceeding.
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



def generate_export_query(start: str, end: str) -> dict:
    """
    Generate an Elasticsearch query dictionary for a specified time range.

    Args:
        start (str): The start time in the required format.
        end (str): The end time in the required format.

    Returns:
        dict: A dictionary representing the query with the time range.
    """
    return {
        "query": {
            "range": {
                "Time": {"format": "strict_date_optional_time", "gte": start, "lt": end}
            }
        }
    }


def generate_delete_query(dateTime_condition_str: str) -> dict:
    return {
        "query": {
            "range": {
                "Time": {
                    "format": "strict_date_optional_time",
                    "lt": dateTime_condition_str,
                }
            }
        }
    }


def delete_documents(es: object, delete_query: dict, index_name: str) -> None:
    response = es.delete_by_query(index=index_name, body=delete_query) #type: ignore
    if "deleted" in response:
        print("Documents deleted succesfully", response["deleted"])
    else:
        print("failed to delete documents")


def create_new_file(earliest_time: str, type: str) -> str:
    """
    Create a new CSV file with the specified earliest time in the filename.

    Args:
        earliest_time (int): The earliest epoch time to include in the filename.

    Returns:
        str: The path to the created CSV file.
    """
    # Ensure the Exports directory exists
    exports_dir = "./Exports"
    if not os.path.exists(exports_dir):
        os.makedirs(exports_dir, exist_ok=True)
        print(f"Created directory: {exports_dir}")

    if type == "custom":
        file_name = f"/Exports/Export_{earliest_time}.csv"
    else:
        file_name = f"/Exports/Export_{earliest_time}.csv"

    # Open file and write the header
    with open(file_name, "w", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
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
            ],
        )
        writer.writeheader()

    return file_name


def fetch_total_records(es, start_time: str, end_time: str) -> int:
    """
    Fetch the total number of records from Elasticsearch.

    Args:
        es (Elasticsearch): The connected Elasticsearch client.
        start_time (str): The start time for the query.
        end_time (str): The end time for the query.

    Returns:
        int: The total number of records.
    """
    query = generate_export_query(start_time, end_time)
    response = es.count(index="wstream_s*", body=query)
    return response["count"]


def fetch_and_write_data_daily(es, start_time, end_time) -> None:
    """
    Fetch data from Elasticsearch and write to CSV files.

    Args:
        es (Elasticsearch): The connected Elasticsearch client.
        start_time (str): The start time for the query.
        end_time (str): The end time for the query.
        directory (str): The directory to save the CSV files.
    """
    query = generate_export_query(start_time, end_time)
    print(query)
    # Initial search to get the first batch of results
    response = es.search(
        index="wstream_s*",
        scroll="1m",
        size=1000,
        _source=[
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
        ],
        body=query,
        sort=[{"Time": {"order": "asc"}}],
    )

    results_count = 0
    earliest_time = min(
        [convert_to_epoch(hit["_source"]["Time"]) for hit in response["hits"]["hits"]]
    )
    current_file = create_new_file(earliest_time, "daily") #type: ignore

    while response["hits"]["hits"]:
        scroll_id = response["_scroll_id"]
        for hit in response["hits"]["hits"]:
            correct_time = convert_to_epoch(hit["_source"]["Time"])
            hit["_source"]["Time"] = correct_time

            # Remove Location field if it exists
            hit["_source"].pop("Location", None)

            with open(current_file, "a", newline="") as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=[
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
                    ],
                )
                writer.writerow(hit["_source"])
                results_count += 1

                # Create a new file if the row count threshold is reached
                if results_count == 500000:
                    results_count = 0
                    earliest_time = hit["_source"]["Time"]
                    print(f"{current_file} Exported")
                    current_file = create_new_file(earliest_time, "daily")

        # Fetch the next batch of results
        response = es.scroll(scroll_id=scroll_id, scroll="1m")


def fetch_and_write_data_custom(
    es, start_time: str, end_time: str, progress_window, total_records: int
) -> None:
    """
    Fetch data from Elasticsearch and write to CSV files.

    Args:
        es (Elasticsearch): The connected Elasticsearch client.
        start_time (str): The start time for the query.
        end_time (str): The end time for the query.
        progress_window (tk): The progress window.
        total_records (int): The total number of records.
    """
    response = es.search(
        index="wstream_smt",
        scroll="1m",
        size=1000,
        _source=[
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
        ],
        body=generate_export_query(start_time, end_time),
        sort=[{"Time": {"order": "asc"}}],
    )
    results_count = 0
    earliest_time = min(
        [convert_to_epoch(hit["_source"]["Time"]) for hit in response["hits"]["hits"]]
    )
    current_file = create_new_file(earliest_time, "custom") #type: ignore

    while response["hits"]["hits"]:
        scroll_id = response["_scroll_id"]
        for hit in response["hits"]["hits"]:
            correct_time = convert_to_epoch(hit["_source"]["Time"])
            hit["_source"]["Time"] = correct_time

            # Remove Location field if it exists
            hit["_source"].pop("Location", None)

            with open(current_file, "a", newline="") as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=[
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
                    ],
                )
                writer.writerow(hit["_source"])
                results_count += 1

                # Create a new file if the row count threshold is reached
                if results_count == 500000:
                    results_count = 0
                    earliest_time = hit["_source"]["Time"]
                    print(f"{current_file} Exported")
                    current_file = create_new_file(earliest_time, "custom")

        # Fetch the next batch of results
        response = es.scroll(scroll_id=scroll_id, scroll="1m")


def find_stop_time(directory: str) -> int:
    """
    Determine the last export time from the CSV files in the given directory.

    Args:
        directory (str): The path to the directory containing export CSV files.

    Returns:
        int: The latest epoch time found in the CSV files.
    """
    files = os.listdir(directory)
    times = [file.split("_")[1].split(".")[0] for file in files]
    max_time = max(times)
    print(f"Latest file: Export_{max_time}.csv")
    # Read the CSV file with the latest time
    df = pd.read_csv(os.path.join(directory, f"Export_{max_time}.csv"))
    print(f"{max(df['Time'])}")
    # Get the maximum time value from the Time column
    return int(max(df["Time"]))


def printMessage(message) -> None:
    msg_box = tk.Toplevel()
    msg_box.geometry("300x100")
    msg_label = tk.Label(msg_box, text=message)
    msg_label.pack(pady=20)
    ok_button = tk.Button(msg_box, text="OK", command=msg_box.destroy)
    ok_button.pack(pady=5)