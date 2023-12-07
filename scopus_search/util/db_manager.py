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

    def find_author(self, author_scopus_id: int):
        return not self.get_author(author_scopus_id=author_scopus_id).empty

    def find_paper(self, paper_scopus_id: int):
        return self.get_paper(paper_scopus_id).empty

    def get_paper(self, paper_scopus_id: int) -> pd.DataFrame:
        return pd.read_sql_query(f"select * from papers where scopus_id={paper_scopus_id}", self.conn)

    def get_author(self, author_scopus_id: int = None, given_name: str = None, surname: str = None) -> pd.DataFrame:
        query = "select * from authors where"

        if author_scopus_id:
            query += f" scopus_id={author_scopus_id}"
        elif given_name and surname:
            query += f" given_name=\"{given_name}\" and surname=\"{surname}\""
        else:
            raise ValueError("Did not receive valid input! ")

        return pd.read_sql_query(query, self.conn)

    def get_papers_by_author(self, author_scopus_id: int, min_year: int = None, max_year: int = None) -> pd.DataFrame:
        query = (f"select scopus_id, title, date from "
                 f"papers join written_by wb on papers.scopus_id = wb.paper "
                 f"where wb.author={author_scopus_id}")

        if min_year:
            query += f" and CAST(substr(date,1,4) as integer) > {min_year}"
        if max_year:
            query += f" and CAST(substr(date,1,4) as integer) < {max_year}"

        query += " order by date desc"

        return pd.read_sql_query(query, self.conn)

    def get_authors_by_paper(self, paper_scopus_id: int) -> tuple:
        return tuple(
            pd.read_sql_query(f"select author from written_by where paper={paper_scopus_id}", self.conn)["author"]
            .tolist())
