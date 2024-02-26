from operator import itemgetter

import numpy as np
import pandas as pd
from itertools import groupby
from elsapy.elsdoc import AbsDoc
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch

from .. import constants as const

COLUMNS = ["scopus_id", "date", "title", "origin", "authors", "from_db", "issn", "issue_id", "page_range", "affiliation", "eid", "isbn"]

def get_paper_authors(df: pd.DataFrame, els_client: ElsClient, author_scopus_id: int) -> tuple:
    db_authors = const.db_manager.get_paper_authors(df.scopus_id)
    if db_authors:
        return db_authors

    doc = AbsDoc(scp_id=df.scopus_id)
    if doc.read(els_client) and "authors" in doc.data:
        return tuple([int(author["@auid"]) for author in doc.data["authors"]["author"]]) or tuple([author_scopus_id])

    # we cant find paper authors without using the author index, so we only include the author we know
    return tuple([author_scopus_id])


# searches the scopus index instead of the authors index, thus paper information might be limited
def get_papers_from_author_by_scopus_search(
        els_client: ElsClient,
        author_scopus_id: int,
        min_year: int = None,
        max_year: int = None) -> (pd.DataFrame, list):
    query = f"AU-ID({author_scopus_id})"

    if max_year:
        query += f" AND PUBYEAR > {max_year - 1}"
    if min_year:
        query += f" AND PUBYEAR < {min_year + 1}"

    search = ElsSearch(query, index="scopus")
    search.execute(els_client)

    if search.results and not ("error" in search.results[0].keys()):
        df = pd.DataFrame(search.results)

        author_guesses = [(uniq, len(list(dups))) for uniq, dups in groupby(sorted(df["dc:creator"].to_list()))]
        author_guesses.sort(key=itemgetter(1), reverse=True)
        author_guesses = [f"[{count}] {name}" for name, count in author_guesses]

        df["dc:identifier"] = df["dc:identifier"].str.replace("SCOPUS_ID:", "").astype(np.int64)
        df.rename(columns={
            "dc:identifier": "scopus_id",
            "prism:coverDate": "date",
            "dc:title": "title",
        }, inplace=True)

        df["from_db"] = df.apply(
            lambda paper: const.db_manager.find_paper(paper.scopus_id), axis=1)

        df["authors"] = df.apply(lambda paper: get_paper_authors(paper, els_client, author_scopus_id), axis=1)
        df["origin"] = "search_api"

        return df[COLUMNS].sort_values(by=['date']), author_guesses

    return pd.DataFrame(), []


def paper_df_from_db_entry(df: pd.DataFrame) -> pd.DataFrame:
    df["authors"] = const.db_manager.get_paper_authors(df["scopus_id"])
    df["from_db"] = True
    return df[COLUMNS]


def get_papers_from_doc_list(doc_list: list) -> pd.DataFrame:
    df = pd.DataFrame(doc_list)

    df["from_db"] = False
    df["origin"] = "authors_api"
    df["scopus_id"] = df["dc:identifier"].str.replace("SCOPUS_ID:", "").astype(np.int64)
    df["authors"] = df["authors"].apply(
        lambda authors: tuple(int(author["authid"]) for author in authors["author"]))

    necessary_columns = [
        "dc:title", "eid", "affiliation",
        "prism:coverDate", "prism:pageRange", "prism:coverDate", "prism:issueIdentifier", "prism:issn", "prism:isbn"]

    df = df.reindex(df.columns.union(necessary_columns, sort=False), axis=1, fill_value=None)

    df = df.rename(columns={
        "dc:title": "title",
        "prism:issn": "issn",
        "prism:isbn": "isbn",
        "prism:coverDate": "date",
        "prism:pageRange": "page_range",
        "prism:issueIdentifier": "issue_id",
    })

    print([[y.keys() for y in x] for x in df["affiliation"]])

    df["affiliation"] = df["affiliation"].apply(
        lambda affiliations: {int(affil["afid"]): affil["affilname"] for affil in affiliations}
    )

    return df[COLUMNS]
