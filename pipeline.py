import csv
import pathlib
import random
from datetime import datetime
import sqlite3

import pytz
from faker import Faker
from typing import TypedDict, Iterator


class UserData(TypedDict):
    id: int
    name: str
    email: str
    password: str
    description: str


def get_user(size: int = 100) -> Iterator[UserData]:
    """Генерирует последовательность словарей с фейковыми данными пользователя.
    
    """
    fake = Faker()
    for i in range(size):
        user: UserData = {
            "id": i,
            "name": fake.first_name(),
            "email": random.choice(
                [fake.email().upper(), fake.email().lower()]
            ),
            "password": fake.password(),
            "description": fake.password(length=20),
        }
        yield user


def generate_csv(path: pathlib.Path, fieldnames: tuple, size: int):
    """Генерирует CSV-файл с данными пользователей.
    
    """
    with path.open("w", encoding="utf-8") as f:
        writer = csv.DictWriter(f=f, fieldnames=fieldnames)
        writer.writeheader()
        for user in get_user(size):
            writer.writerow(user)
    print(f"{size} records written to a file {path}.")


def init_db(path: pathlib.Path):
    """Инициализирует базу данных SQLite.

    """
    if pathlib.Path(path).exists() and pathlib.Path(path).is_file():
        pathlib.Path(path).unlink()

    query = """
    CREATE TABLE IF NOT EXISTS users (
    id INT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    description TEXT NOT NULL,
    processed_at TEXT NOT NULL
    );
    """

    conn = sqlite3.connect(path)

    try:
        with conn:
            cur = conn.cursor()
            cur.execute(query)
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}.")
    else:
        print("Table created successfull.")
    finally:
        conn.close()


def extract_users(path: pathlib.Path):
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("E: start extract")
        for i, row in enumerate(reader):
            print(f"E: extract row #{i}")
            yield row
        print("E: extract finished.")


def transform_users(row_generator):
    print("T: start transform")
    for i, row in enumerate(row_generator):
        print(f"T: transform row #{i}")
        del row["password"]
        row["id"] = int(row["id"])
        row["email"] = row["email"].lower()
        row["description"] = "".join(
            char for char in row["description"] if char.isalnum()
        )
        row["processed_at"] = datetime.now(tz=pytz.timezone("UTC"))
        yield row
    print("T: transform finished.")


def load_data(row_generator, path: pathlib.Path):
    query = """
    INSERT INTO users (
    id,
    name,
    email,
    description,
    processed_at
    )
    VALUES (?, ?, ?, ?, ?)
    """

    conn = sqlite3.connect(path)

    try:
        with conn:
            cur = conn.cursor()
            print("L: start load")
            for row in row_generator:
                print(f"L: insert row {row}")
                cur.execute(query, tuple(row.values()))
        print("L: load finished.")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}.")
    finally:
        conn.close()


csv_file = pathlib.Path("/tmp/users.csv")
csv_file_fieldnames = ("id", "name", "email", "password", "description")
db_path = pathlib.Path("/tmp/users.db")
num_records = 20

if __name__ == "__main__":
    # 1. Генерация данных:
    generate_csv(csv_file, csv_file_fieldnames, num_records)
    # 2. Инициализация БД
    init_db(db_path)
    # 3. Инициализация генератора extract (E)
    extract_data = extract_users(csv_file)
    # 4. Инициализация генератора transform (T)
    transform_data = transform_users(extract_data)
    # 5. Загрузка данных (L)
    load_data(transform_data, db_path)
