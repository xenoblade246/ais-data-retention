# ilm_manager.py

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

# Import from your updated aisUtil
from aisUtil import (
    setup_ilm_for_all_indices,
    check_ilm_status,
    manual_delete_old_data,
    create_daily_cleanup_task
)

def create_index_template(es, mappings: dict, template_name="ais_data_template", index_pattern="wstream_s*", 
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

def main_menu():
    """Main menu for ILM management"""
    print("\n" + "="*60)
    print("ELASTICSEARCH ILM MANAGER")
    print("Automatically delete data older than 2 weeks")
    print("="*60)
    
    user, password = setup_environment()
    if not user or not password:
        return
    
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

def schedule_daily_cleanup():
    """
    Create a scheduled task for daily cleanup.
    This can be added to cron (Linux) or Task Scheduler (Windows).
    """
    user, password = setup_environment()
    if not user or not password:
        return
    
    print(f"\n📅 Scheduling daily cleanup at {datetime.now()}")
    print("This function should be scheduled to run daily.")
    print("\nFor Linux (cron):")
    print("0 2 * * * /usr/bin/python3 /path/to/ilm_manager.py --daily-cleanup")
    print("\nFor Windows (Task Scheduler):")
    print("Create a daily task at 2 AM running:")
    print('python "C:\\path\\to\\ilm_manager.py" --daily-cleanup')
    
    # Run the cleanup
    create_daily_cleanup_task(user, password)

if __name__ == "__main__":
    # Command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--daily-cleanup":
            schedule_daily_cleanup()
        elif sys.argv[1] == "--setup":
            user, password = setup_environment()
            if user and password:
                setup_ilm_for_all_indices(user, password, 14)
        elif sys.argv[1] == "--check":
            user, password = setup_environment()
            if user and password:
                check_ilm_status(user, password)
        elif sys.argv[1] == "--manual-cleanup":
            user, password = setup_environment()
            if user and password:
                manual_delete_old_data(user, password, 14)
        else:
            print("Usage:")
            print("  python ilm_manager.py                     # Interactive menu")
            print("  python ilm_manager.py --daily-cleanup     # Run daily cleanup")
            print("  python ilm_manager.py --setup             # Setup ILM policy")
            print("  python ilm_manager.py --check             # Check ILM status")
            print("  python ilm_manager.py --manual-cleanup    # Manual cleanup")
    else:
        # Interactive mode
        main_menu()