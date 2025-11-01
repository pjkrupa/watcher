# script to extract existing logs and load them into the db
import os, gzip, shutil, fnmatch, argparse
from dotenv import load_dotenv
from typing import List
from psycopg2.extensions import cursor
from watch import make_table, write_to_db, db_connect

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--path",
        help="path of the directory containing the files to be extracted"
    )
    return parser

# gets a list of all .gz files that begin with "access" and end in ".gz"
def get_file_list(path:str) -> List[str]:
    
    # this is for Nginx logs:
    #files = [x for x in os.listdir(path=path) if fnmatch.fnmatch(x, "access.*.gz")]
    
    # this is for testing:
    files = [x for x in os.listdir(path=path) if fnmatch.fnmatch(x, "*.gz")]
    return files

# extracts a file, saves it to a temp folder, returns num of lines
def extract_file(path:str, name:str, temp:str) -> str:
    write_name = name.strip(".gz")
    read_path = os.path.join(path, name)
    write_path = os.path.join(temp, write_name)
    
    try:
        with open(read_path, 'rb') as f_in:
            with gzip.open(f_in, 'rb') as f_gz:
                with open(write_path, 'wb') as f_out:
                    shutil.copyfileobj(f_gz, f_out)
        print(f"Extracted {read_path} to {write_path}")
        return write_path
    except Exception as e:
        print(f"Something went wrong: {e}")
        return None

# loads an extracted file into the db, return num of records saved
def load_db(path:str, cur:cursor) -> int:
    records = 0
    with open(path, 'r') as f:
        for line in f:
            write_to_db(line=line, cur=cur)
            records += 1
    return records

# checks db size, returns how many records were loaded
def checker(cur:cursor) -> int:
    cur.execute("SELECT COUNT(*) FROM logs")
    new_count = cur.fetchone()[0]
    return new_count

# removes the temp files
def cleanup():
    pass

if __name__ == "__main__":
    # doing the setup
    load_dotenv()
    print(f"Database name from .env: {os.getenv("DATABASE_NAME")}")
    parser = get_parser()
    args = parser.parse_args()
    cur = db_connect()
    make_table(cur)
    os.makedirs("temp", exist_ok=True)

    # fetching the names of the files to be extracted
    files = get_file_list(args.path)
    extracted = []
    for file in files:
        file_path = os.path.join(args.path, file)
        extracted.append(extract_file(path=args.path, name=file, temp="temp"))
    print(f"Extracted {len(extracted)} of {len(files)} log files.")
    print(f"Loading to db...")
    for file in extracted:
        try:
            record_count = load_db(path=file, cur=cur)
            print(f"Loaded {record_count} records from {file}")
        except Exception as e:
            print(f"Something went wrong: {e}")

    print(f"cleaning up temp folder...")
    shutil.rmtree("temp")
    r_count = checker(cur)
    print(f"A total of {r_count} records added to the database.")

