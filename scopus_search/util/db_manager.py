import sqlite3
from datetime import datetime

import pandas as pd

_CREATE_AUTHORS_QUERY = """
create table if not exists authors
(
    scopus_id integer not null
        constraint authors_pk
            primary key,
    given_name TEXT,
    surname TEXT,
    base_id INTEGER DEFAULT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""

_CREATE_PAPERS_QUERY = """
create table if not exists papers
(
    scopus_id integer
        constraint papers_pk
            primary key,
    title       TEXT,
    date        TEXT,
    origin      TEXT,
    affiliation TEXT,
    page_range  TEXT,
    issue_id    TEXT,
    issn        TEXT,
    isbn        TEXT,
    eid         TEXT,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_AFFILIATIONS_QUERY = """
create table if not exists affiliations
(
    afid integer
        constraint papers_pk,
    afilname  TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""

_CREATE_WRITTEN_BY_QUERY = """
create table if not exists written_by
(
    author integer not null
        constraint written_by_authors_scopus_id_fk
            references authors,
    paper integer not null
        constraint written_by_papers_scopus_id_fk
            references papers
);"""

_CREATE_AFFILIATED_TO_QUERY = """
create table if not exists affiliated_to
(
    afil integer not null
        constraint affiliated_to_affil_afid_fk
            references affiliations,
    paper integer not null
        constraint affiliated_to_papers_scopus_id_fk
            references papers
);"""

_CREATE_WRITTEN_BY_IDX_QUERY = "CREATE UNIQUE INDEX IF NOT EXISTS written_by_uniq ON written_by (author, paper);"

class DbManager:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._setup_tables()

    def _setup_tables(self):
        self.cursor.execute(_CREATE_PAPERS_QUERY)
        self.cursor.execute(_CREATE_AUTHORS_QUERY)
        self.cursor.execute(_CREATE_WRITTEN_BY_QUERY)
        self.cursor.execute(_CREATE_AFFILIATIONS_QUERY)
        self.cursor.execute(_CREATE_AFFILIATED_TO_QUERY)
        self.cursor.execute(_CREATE_WRITTEN_BY_IDX_QUERY)

    def insert_scopus_author(self, scopus_id, given_name, surname, base_id: int = None):
        self.cursor.execute("insert into authors (base_id, scopus_id, given_name, surname, updated_at) "
                            "values (?,?,?,?, current_timestamp) on conflict do nothing",
                            [base_id, scopus_id, given_name, surname])
        self.conn.commit()

    def insert_paper_df(self, papers_df: pd.DataFrame):
        papers_df["from_db"] = papers_df.apply(lambda paper: self.find_paper(paper.scopus_id), axis=1)
        papers_df = papers_df[papers_df["from_db"] == False]
        papers_df = papers_df.drop_duplicates(subset="scopus_id")

        authors_df = papers_df[["scopus_id", "authors"]].explode("authors")
        authors_df.rename(
            columns={
                "authors": "author",
                "scopus_id": "paper"
            }, inplace=True)
        authors_df.drop_duplicates(inplace=True)
        authors_df.to_sql("written_by", self.conn, if_exists="append", index=False)

        def extract_afilname(affiliation):
            affiliation["afilname"] = affiliation["afil_dict"][affiliation["afid"]]
            return affiliation[["afid", "afilname"]]

        affiliations = papers_df[["scopus_id", "affiliation"]].copy()
        affiliations["afil_dict"] = affiliations["affiliation"]

        affiliations = affiliations.explode("affiliation")
        affiliations.rename(columns={"affiliation": "afid"}, inplace=True)

        affiliated_to = affiliations[["scopus_id", "afid"]].copy()

        affiliations = affiliations.apply(extract_afilname, axis=1)

        affiliations = affiliations[affiliations.apply(lambda affiliation: self.find_afil(affiliation["afid"]), axis=1) == False]

        affiliations["updated_at"] = str(datetime.utcnow())
        affiliations.to_sql("affiliations", self.conn, if_exists="append", index=False)

        affiliated_to.rename(columns={
            "scopus_id": "paper",
            "afid": "afil"
        }, inplace=True)

        affiliated_to.to_sql("affiliated_to", self.conn, if_exists="append", index=False)

        papers_df = papers_df[["scopus_id", "title", "date", "origin", "page_range", "issue_id", "issn", "isbn", "eid"]]

        # TODO fix timezone difference
        update_time = str(datetime.utcnow())
        papers_df["updated_at"] = update_time
        papers_df.to_sql("papers", self.conn, if_exists="append", index=False)

        self.conn.commit()

    def is_base_author(self, scopus_id: int) -> bool:
        author = self.get_scopus_author(scopus_id=scopus_id)
        return (author["base_id"][0] is not None) if not author.empty \
            else False

    def find_author_by_name(self, given_name: str, surname: str):
        return not self.get_scopus_author(given_name=given_name, surname=surname).empty

    def find_author(self, author_scopus_id: int):
        return not self.get_scopus_author(scopus_id=author_scopus_id).empty

    def find_paper(self, paper_scopus_id: int):
        return not self.get_paper(paper_scopus_id).empty

    def find_afil(self, afid: int):
        return not self.get_afil(afid).empty

    def get_paper(self, paper_scopus_id: int) -> pd.DataFrame:
        return pd.read_sql_query(f"select * from papers where scopus_id={paper_scopus_id}", self.conn)

    def get_scopus_author(self, scopus_id: int = None, given_name: str = None, surname: str = None) -> pd.DataFrame:
        query = "select * from authors where"

        if scopus_id:
            query += f" scopus_id={scopus_id}"
        elif given_name and surname:
            query += f" given_name=\"{given_name}\" and surname=\"{surname}\""
        else:
            raise ValueError("Did not receive valid input!")

        return pd.read_sql_query(query, self.conn)

    def get_author_scopus_ids(self, scopus_id: int) -> pd.DataFrame:
        base_id = self.get_scopus_author(scopus_id=scopus_id)["base_id"][0] or scopus_id
        return pd.read_sql_query(
            f"select scopus_id, given_name, surname from authors "
            f"where (base_id={base_id}) or (scopus_id={base_id}) order by base_id nulls first",
            self.conn)

    def get_last_updated_paper(self, author_scopus_id: int):
        return pd.read_sql_query(f"select date from papers left join written_by on papers.scopus_id = written_by.paper "
                                 f"where written_by.author = {author_scopus_id} order by date desc limit 1", self.conn)

    def get_papers_by_scopus_author(self, author_scopus_id: int, min_year: int = None, max_year: int = None) -> pd.DataFrame:
        query = ("select * from papers "
                 "left join written_by on papers.scopus_id = written_by.paper "
                 f"where written_by.author = {author_scopus_id}")

        if min_year:
            query += f" and CAST(strftime('%Y', date) as integer) >= {min_year}"
        if max_year:
            query += f" and CAST(strftime('%Y', date) as integer) < {max_year}"

        query += " order by date desc"

        return pd.read_sql_query(query, self.conn)

    def get_paper_authors(self, paper_scopus_id: int) -> tuple:
        return tuple(
            pd.read_sql_query(f"select author from written_by where paper={paper_scopus_id}", self.conn)["author"]
            .tolist())

    def get_afil(self, afid):
        return pd.read_sql_query(f"select * from affiliations where afid={afid}", self.conn)
