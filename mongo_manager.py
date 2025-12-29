import os
import sys
import subprocess

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv('config/.env')
URI = os.getenv("URI")
SYSTEM_DBS = {"admin", "local", "config"}

def transfer_database(source_uri: str, target_uri: str, db: str,):

    tgt = MongoClient(target_uri)
    tgt.drop_database(db)

    dump_cmd = [
        "mongodump",
        "--archive",
        "--gzip",
        f"--uri={source_uri}",
        f"--db={db}"
    ]

    restore_cmd = [
        "mongorestore",
        "--archive",
        "--gzip",
        f"--uri={target_uri}"
    ]

    print("========================================")
    print("Running Dump:")
    print("  ", " ".join(dump_cmd))
    print()
    print("Running Restore:")
    print("  ", " ".join(restore_cmd))
    print()

    p1 = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(restore_cmd, stdin=p1.stdout)

    p1.stdout.close()

    p2_ret = p2.wait()
    p1_ret = p1.wait()

    if p1_ret != 0 or p2_ret != 0:
        raise RuntimeError(f"mongodump exited with {p1_ret}, mongorestore exited with {p2_ret}")

    print(f"Transferred '{db}' From '{source_uri}' To '{target_uri}' Completed")

def host_to_host():

    while True:

        print("==========================================")
        print("  MongoDB Host-to-Host Database Transfer  ")
        print("==========================================")

        source_uri = input("Enter Source URI: ").strip()
        target_uri = input("Enter Target URI: ").strip()

        if not source_uri or not target_uri:
            print("[!!!] Source and Target URI cannot be empty.\n")
            continue

        client      = MongoClient(source_uri)
        db_names = sorted([d for d in client.list_database_names() if d not in SYSTEM_DBS])

        if not db_names:
            print("[!!!] No non-system databases found on source.\n")
            continue

        print("======================================")
        print("     Select Database to Transfer:     ")
        print("======================================")

        for idx, coll in enumerate(db_names):
            print(f"[{idx + 1:02d}] {coll}")

        print("[C] Cancel")
        choice = input(f"Select an option [1-{len(db_names)}/C]: ").strip()

        if choice == "C":
            print("Cancelled")
            sys.exit(0)

        elif not choice.isdigit() or int(choice) not in range(1, len(db_names) + 1):
            print("========================================")
            print("[!!!] Invalid choice. Please try again.")
            continue

        db_name = db_names[int(choice) - 1]
        transfer_database(source_uri, target_uri, db_name)
        break

def transfer_collection(tt_type: str,collection: str):

    if tt_type == "1":
        db_from = "Modoo_data" ; db_to = "Archived"
        # source_uri = os.getenv("URI_MAIN") ; target_uri = os.getenv("URI_ARCHIVE")
    elif tt_type == "2":
        db_from = "Archived"; db_to = "Modoo_data"
        # source_uri = os.getenv("URI_ARCHIVE") ; target_uri = os.getenv("URI_MAIN")

    dump_cmd = [
        "mongodump",
        "--archive",
        f"--uri={URI}",
        f"--db={db_from}",
        f"--collection={collection}"
    ]

    restore_cmd = [
        "mongorestore",
        "--archive",
        f"--uri={URI}",
        f"--nsFrom={db_from}.{collection}",
        f"--nsTo={db_to}.{collection}",
        "--drop"
    ]

    print("========================================")
    print("Running Dump:")
    print("  ", " ".join(dump_cmd))
    print()
    print("Running Restore:")
    print("  ", " ".join(restore_cmd))
    print()

    p1 = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(restore_cmd, stdin=p1.stdout)

    p1.stdout.close()

    p2_ret = p2.wait()
    p1_ret = p1.wait()

    if p1_ret != 0 or p2_ret != 0:
        raise RuntimeError(f"mongodump exited with {p1_ret}, mongorestore exited with {p2_ret}")

    print("========================================")
    print(f"Transfer '{collection}' From '{db_from}' To '{db_to}' Completed")

    return True

def db_to_db_collections(tt_type: str):

    if tt_type == "1":
        db_from = "Modoo_data"
    elif tt_type == "2":
        db_from = "Archived"

    client      = MongoClient(URI)
    db          = client[db_from]
    coll_names  = db.list_collection_names()

    coll_names.sort()

    while True:

        print("========================================")
        print("     Select Collection to Transfer:     ")
        print("========================================")
        for idx, coll in enumerate(coll_names):
            print(f"[{idx+1:02d}] {coll}")

        print("[C] Cancel")
        choice = input(f"Select an option [1-{len(coll_names)}/C]: ").strip()

        if choice == "C":
            print("Cancelled")
            sys.exit(0)

        elif not choice.isdigit() or int(choice) not in range(1, len(coll_names) + 1):
            print("========================================")
            print("[!!!] Invalid choice. Please try again.")
            continue

        coll_name = coll_names[int(choice) - 1]
        if transfer_collection(tt_type, coll_name):
            db[coll_name].drop()
            break

def db_to_db():

    while True:

        print("========================================")
        print("  MongoDB DB-to-DB Collection Transfer  ")
        print("========================================")
        print("[1] Transfer from 'Modoo_data' to 'Archived'")
        print("[2] Transfer from 'Archived' to 'Modoo_data'")
        print("[C] Cancel")

        choice = input("Select an option [1/2/C]: ").strip()

        if choice == "1" or choice == "2":
            db_to_db_collections(tt_type=choice)
        elif choice == "C":
            print("Cancelled")
            sys.exit(0)
        else:
            print("[!!!] Invalid choice. Please select 1, 2, or 3.\n")

def main():

    while True:

        print("===========================")
        print("  MongoDB Manager Service  ")
        print("===========================")
        print("[1] DB-to-DB Collection Transfer")
        print("[2] Host-to-Host Database Transfer")
        print("[C] Cancel")

        choice = input("Select an option [1/2/C]: ").strip()

        if choice == "1":
            db_to_db()
        elif choice == "2":
            host_to_host()
        elif choice == "C":
            print("Cancelled")
            sys.exit(0)
        else:
            print("[!!!] Invalid choice. Please select 1, 2, or 3.\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled")
        sys.exit(1)