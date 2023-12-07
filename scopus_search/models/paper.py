import numpy as np
import pandas as pd
from elsapy.elsdoc import AbsDoc
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch

from .. import constants as const


def get_authors_by_paper_df(df: pd.DataFrame, els_client: ElsClient) -> tuple:
    if const.db_manager.find_paper(df.scopus_id):
        return const.db_manager.get_authors_by_paper(df.scopus_id)

    doc = AbsDoc(scp_id=df.scopus_id)
    if doc.read(els_client) and "authors" in doc.data:
        return tuple([int(author["@auid"]) for author in doc.data["authors"]["author"]])

    return ()  # we cant find paper authors without using the author index, so we keep the authors emtpy


# searches the scopus index instead of the authors index, thus paper information might be limited
def get_papers_from_author_by_scopus_search(
        els_client: ElsClient,
        author_scopus_id: int,
        min_year: int = None,
        max_year: int = None):
    query = f"AU-ID({author_scopus_id})"

    if max_year:
        query += f" AND PUBYEAR > {max_year - 1}"
    if min_year:
        query += f" AND PUBYEAR < {min_year + 1}"

    search = ElsSearch(query, index="scopus")
    search.execute(els_client)

    if search.results and not ("error" in search.results[0].keys()):
        df = pd.DataFrame(search.results, columns=const.SCOPUS_SEARCH_KEYS)

        df["from_db"] = False
        df["dc:identifier"] = df["dc:identifier"].str.replace("SCOPUS_ID:", "").astype(np.int64)
        df.rename(columns={
            "dc:identifier": "scopus_id",
            "prism:coverDate": "date",
            "dc:title": "title",
        }, inplace=True)

        df["authors"] = df.apply(lambda paper: get_authors_by_paper_df(paper, els_client), axis=1)

        return df.sort_values(by=['date'])

    return pd.DataFrame()


def paper_df_from_db_entry(df: pd.DataFrame) -> pd.DataFrame:
    df["authors"] = const.db_manager.get_authors_by_paper(df["scopus_id"])
    df["from_db"] = True
    return df


def get_papers_from_doc_list(doc_list: list) -> pd.DataFrame:
    df = pd.DataFrame(doc_list)

    df["from_db"] = False
    df["dc:identifier"] = df["dc:identifier"].str.replace("SCOPUS_ID:", "").astype(np.int64)
    df["authors"] = df["authors"].apply(
        lambda authors: tuple(int(author["authid"]) for author in authors["author"]))

    return df.rename(columns={
        "dc:identifier": "scopus_id",
        "prism:coverDate": "date",
        "dc:title": "title",
    })


def save_paper_to_db(paper: pd.Series):
    if not paper.from_db:
        const.db_manager.insert_paper(paper.scopus_id, paper.title, paper.date)
        for author in paper.authors:
            const.db_manager.insert_written_by(author, paper.scopus_id)
    return paper
