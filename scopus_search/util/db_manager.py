import sqlite3
import pandas as pd

_CREATE_AUTHORS_QUERY = """
create table if not exists authors
(
    scopus_id  integer not null
        constraint authors_pk
            primary key,
    given_name TEXT,
    surname  TEXT
);"""

_CREATE_PAPERS_QUERY = """
create table if not exists papers
(
    scopus_id integer
        constraint papers_pk
            primary key,
    title     TEXT,
    date      TEXT
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
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._setup_tables()

    def _setup_tables(self):
        self.cursor.execute(_CREATE_PAPERS_QUERY)
        self.cursor.execute(_CREATE_AUTHORS_QUERY)
        self.cursor.execute(_CREATE_WRITTEN_BY_QUERY)

    def insert_author(self, scopus_id: int, given_name: str, surname: str):
        self.cursor.execute("insert or replace into authors values (?,?,?)", (scopus_id, given_name, surname))
        self.conn.commit()

    def insert_written_by(self, author_id: int, paper_id: int):
        self.cursor.execute("insert or replace into written_by values (?,?)", [author_id, paper_id])
        self.conn.commit()

    def insert_paper(self, scopus_id: int, title: str, date: str):
        self.cursor.execute("insert or replace into papers values (?,?,?)", [scopus_id, title, date])
        self.conn.commit()