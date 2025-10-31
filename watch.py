import argparse, os, select
import psycopg2
from psycopg2.extensions import cursor
from dotenv import load_dotenv

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
            rlist, _, _ = select.select([f], [], [], 1.0)
            if rlist:
                for line in f:
                    yield line.strip()

def make_table(cur: cursor ):
    cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            line TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

def write_to_db(cur: cursor):
    cur.execute("INSERT INTO logs (line) VALUES (%s)", (line,))

if __name__ == "__main__":
    load_dotenv()
    parser = get_parser()
    args = parser.parse_args()
    with psycopg2.connect(
        dbname=os.getenv('DATABASE_NAME'),
        user=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASSWORD')
        ) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        make_table(cur)
        for line in follow(args.file):
            if line:
                write_to_db(line=line, cur=cur)
