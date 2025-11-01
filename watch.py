import argparse, os, time
import psycopg2
from psycopg2.extensions import cursor
from dotenv import load_dotenv

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        help="path of the file to monitor"
    )
    return parser


def follow(filepath: str):
    with open(filepath, "r") as f:
        f.seek(0, 2)
        ino = os.fstat(f.fileno()).st_ino
        while True:
            lines = f.readlines()
            if lines:
                yield from (line.strip() for line in lines if line.strip())
            else:
                time.sleep(10)
                try:
                    # if inode changed, reopen the file
                    if os.stat(filepath).st_ino != ino:
                        f.close()
                        f = open(filepath, "r")
                        ino = os.fstat(f.fileno()).st_ino
                        print("Detected log rotation, reopening file...")
                except FileNotFoundError:
                    # file temporarily missing during rotation
                    time.sleep(10)



def make_table(cur: cursor ):
    cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            line TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

def write_to_db(cur: cursor, line: str):
    cur.execute("INSERT INTO logs (line) VALUES (%s)", (line,))
    print(f'wrote "{line}" to the database')

def db_connect():
    with psycopg2.connect(
        dbname=os.getenv('DATABASE_NAME'),
        user=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASSWORD'),
        host=os.getenv('DATABASE_HOST', 'localhost'),
        port=os.getenv('DATABASE_PORT', '5432')
        ) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        return cur

if __name__ == "__main__":
    load_dotenv()
    parser = get_parser()
    args = parser.parse_args()
    cur = db_connect()
    make_table(cur)
    for line in follow(args.file):
        if line:
            write_to_db(line=line, cur=cur)
