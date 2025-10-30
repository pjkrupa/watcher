import time, argparse, os
import psycopg2
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class Database:
    name: str
    user: str
    pw: str

def get_parser():
    parser = argparse.ArgumentParser
    parser.add_argument(
        "f",
        "--file",
        help="path of the file to monitor"
    )
    return parser


def follow(filepath: str):
    with open(filepath, "r") as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue
            yield line.strip()

def make_db(db: Database):
    with psycopg2.connect(
        dbname=db.name,
        user=db.user,
        password=db.pw
        ) as conn:
        cur = conn.cursor()
        cur.execute()

# this should return something, handle errors, and do logging
def write_to_db(line: str, db: Database) -> bool:
    with psycopg2.connect(
        dbname=db.name,
        user=db.user,
        password=db.pw
        ) as conn:
        cur = conn.cursor()
        cur.execute()

if __name__ == "__main__":
    load_dotenv()
    db = Database(
        name = os.getenv('DATABASE_NAME')
        user = os.getenv('DATABASE_USER')
        pw = os.getenv('DATABASE_PASSWORD')
        )
    

    parser = get_parser()
    args = parser.parse_args()
    for line in follow(args.file):
        if line:
            print(f"New line: {line}")
