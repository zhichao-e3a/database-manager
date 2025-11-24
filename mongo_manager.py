import os
import sys
import subprocess

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv('config/.env')

URI = os.getenv("URI")

def transfer_collection(
    tt_type: str,
    collection: str,
):

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

def transfer(tt_type: str):

    if tt_type == "1":
        db_from = "Modoo_data" ; db_to = "Archived"
    elif tt_type == "2":
        db_from = "Archived" ; db_to = "Modoo_data"

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

def main():

    while True:

        print("========================================")
        print("  MongoDB DB-to-DB Collection Transfer  ")
        print("========================================")
        print("[1] Transfer from 'Modoo_data' to 'Archived'")
        print("[2] Transfer from 'Archived' to 'Modoo_data'")
        print("[C] Cancel")

        choice = input("Select an option [1/2/C]: ").strip()

        if choice == "1" or choice == "2":
            transfer(tt_type=choice)
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