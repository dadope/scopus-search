from parse import parse
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
from elsapy.elsprofile import ElsAuthor

from .. import constants as const
from .paper import get_paper_from_search_entry


def _extract_names_from_full_name(full_name: str, full_name_format: str) -> (str, str):
    try:
        r = parse(full_name_format, full_name)
        return r["given_name"].strip(), r["surname"].strip()
    except Exception as e:
        raise ValueError("Could not parse first and last name from full name! ran into exception " + str(e))


class Author:
    def __init__(self,
                 client: ElsClient,
                 surname: str = None,
                 scopus_id: int = None,
                 full_name: str = None,
                 given_name: str = None,
                 input_format: str = const.DEFAULT_NAME_INPUT_FORMAT,
                 output_format: str = const.DEFAULT_NAME_OUTPUT_FORMAT):

        if not (scopus_id or full_name or (given_name and surname)):
            raise ValueError("Did not receive scopus id or full name or first and last names of the author")

        self._els_client = client
        self.output_format = output_format

        if full_name and not (given_name and surname):
            self.given_name, self.surname = _extract_names_from_full_name(full_name, input_format)

        self.scopus_id = scopus_id or self._get_scopus_id_by_name()
        self._scopus_author = ElsAuthor(author_id=self.scopus_id)

        if not (given_name and surname):
            self.given_name, self.surname = self._get_author_name_by_scopus_id()

        if not self._scopus_author.read_docs(self._els_client):
            raise ValueError("Could not find author papers, please check your api key permissions")

        self.papers = [get_paper_from_search_entry(doc) for doc in self._scopus_author.doc_list]

        self.key = self._get_key()
        self._save_to_db()

    def _get_key(self) -> str:
        try:
            return self.output_format.format(**dict(
                scopus_id=self.scopus_id,
                given_name=self.given_name,
                surname=self.surname
            ))
        except KeyError:
            return const.DEFAULT_NAME_OUTPUT_FORMAT.format(**dict(given_name=self.given_name, surname=self.surname))

    def _save_to_db(self):
        const.db_manager.insert_author(self.scopus_id, self.given_name, self.surname)
        for paper in self.papers:
            const.db_manager.insert_paper(paper.scopus_id, paper.title, paper.cover_date)
            const.db_manager.insert_written_by(self.scopus_id, paper.scopus_id)

    def _get_scopus_id_by_name(self):
        query = f"AUTHFIRST({self.given_name}) AND AUTHLASTNAME({self.surname})"
        search = ElsSearch(query, index="author")

        search.execute(self._els_client)

        if not search.results or "error" in search.results[0].keys():
            raise ValueError("Could not find author scopus id, please check your api key permissions")

        # TODO: find best matching author instead of just taking the first result
        search_entry = search.results[0]

        return int(search_entry["dc:identifier"].replace("AUTHOR_ID:", ""))

    def _get_author_name_by_scopus_id(self):
        if not self._scopus_author.read(self._els_client):
            raise ValueError("Could not find author names, please check your api key permissions")

        return self._scopus_author.first_name, self._scopus_author.last_name
