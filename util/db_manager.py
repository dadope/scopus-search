import sqlite3
from pathlib import Path

_res_dir = Path.cwd() / "resources"
_res_dir.mkdir(exist_ok=True)

DB_PATH = str(_res_dir / "database.db")

_CREATE_AUTHORS_QUERY = """
create table if not exists authors
(
    scopus_id  integer not null
        constraint authors_pk
            primary key,
    first_name TEXT,
    last_name  TEXT
);"""

_CREATE_PAPERS_QUERY = """
create table if not exists papers
(
    scopus_id integer
        constraint papers_pk
            primary key,
    title     TEXT,
    year      integer
);
"""

_CREATE_WRITTEN_BY_QUERY = """
create table if not exists written_by
(
    author integer
        constraint written_by_authors_scopus_id_fk
            references authors,
    paper  integer
        constraint written_by_papers_scopus_id_fk
            references papers
);"""


class DbManager:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        self._setup_tables()

    def _setup_tables(self):
        self.cursor.execute(_CREATE_PAPERS_QUERY)
        self.cursor.execute(_CREATE_AUTHORS_QUERY)
        self.cursor.execute(_CREATE_WRITTEN_BY_QUERY)

    def insert_author(self, scopus_id: int, first_name: str, last_name: str):
        self.cursor.execute("insert or replace into authors values (?,?,?)", (scopus_id, first_name, last_name))
        self.conn.commit()

    def insert_written_by(self, author_id: int, paper_id: int):
        self.cursor.execute("insert or replace into written_by values (?,?)", [author_id, paper_id])
        self.conn.commit()

    def insert_paper(self, scopus_id: str, title: str, year: int):
        self.cursor.execute("insert or replace into papers values (?,?,?)", [scopus_id, title, year])
        self.conn.commit()