import pandas as pd
from parse import parse
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor
from elsapy.elssearch import ElsSearch

from .scopus_author import ScopusAuthor
from ..util.commandline_util import log_and_print_if_verbose
from .. import constants as const


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
                 scopus_ids_to_exclude: list = None,
                 verbose: bool = False, ask_user_input: bool = False,
                 input_format: str = const.DEFAULT_NAME_INPUT_FORMAT, output_format: str = const.DEFAULT_NAME_OUTPUT_FORMAT):
        self.verbose = verbose
        self.output_format = output_format
        self.ask_user_input = ask_user_input
        self._els_client = client
        self._ids_to_exclude = scopus_ids_to_exclude or []

        if not (scopus_id or full_name or (given_name and surname)):
            raise ValueError("Did not receive scopus id or full name or first and last names of the author")

        if scopus_id:
            self.scopus_authors = [
                ScopusAuthor(client, scopus_id, output_format=output_format, verbose=verbose, ask_user_input=ask_user_input)]
        else:
            if full_name:
                given_name, surname = _extract_names_from_full_name(full_name, input_format)

            self.scopus_authors = self._get_scopus_authors_by_name(given_name, surname)

            if len(self.scopus_authors) < 1:
                raise ValueError("Could not find any authors or excluded too many!")

        self.base_author = self.scopus_authors[0]
        self._save_to_db()

    def filter_papers(self,
                      max_year: int = None,
                      min_year: int = None,
                      include_authors: list[int] = [],
                      include_all_authors: list[int] = [],
                      not_include_authors: list[int] = []):
        for author in self.scopus_authors:
            author.filter_papers(max_year, min_year, include_authors, include_all_authors, not_include_authors)

    def _get_scopus_authors_by_name(self, given_name: str, surname: str) -> list[ScopusAuthor]:
        log_and_print_if_verbose(
            f"Finding scopus_id by first ({given_name}) and last ({surname}) name",
            self.verbose)

        if const.db_manager.find_author_by_name(given_name, surname):
            return [
                ScopusAuthor(
                    client=self._els_client,
                    scopus_id=author.scopus_id,
                    given_name=author.given_name,
                    surname=author.surname,
                    output_format=self.output_format, verbose=self.verbose, ask_user_input=self.ask_user_input
                ) for author in const.db_manager.get_author_scopus_ids(
                    int(const.db_manager.get_scopus_author(given_name=given_name, surname=surname)["scopus_id"][0])
                ).itertuples() if author.scopus_id not in self._ids_to_exclude
            ]

        query = f"AUTHFIRST({given_name}) AND AUTHLASTNAME({surname})"
        search = ElsSearch(query, index="author")

        try:
            search.execute(self._els_client)
        except Exception:
            # TODO: consider workaround thorough the scopus api
            # query = f"AUTHOR-NAME({self.surname}, {self.given_name[:1]})"
            # search = ElsSearch(query, index="scopus")
            # try:
            #    search.execute(self._els_client)
            # except Exception as e:
            #    raise ValueError("Could not find author scopus id, please check your api key permissions")
            raise ValueError("Could not find author scopus id, please check your api key permissions")

        if not getattr(search, "results") or "error" in search.results[0].keys():
            raise ValueError("Could not find author scopus id, please check your api key permissions")

        log_and_print_if_verbose(f"Found {len(search.results)} entries!", self.verbose)

        return [
            ScopusAuthor(
                self._els_client,
                scopus_id=scopus_id,
                output_format=self.output_format, verbose=self.verbose, ask_user_input=self.ask_user_input)
            for scopus_id in [int(entry["dc:identifier"].replace("AUTHOR_ID:", "")) for entry in search.results]
        ]

    def _save_to_db(self):
        log_and_print_if_verbose(f"Saving author: {self.base_author.scopus_id} and papers to database...", self.verbose)
        for author in self.scopus_authors:
            if not const.db_manager.find_author(author.scopus_id):
                base_id = None if author is self.base_author \
                    else self.base_author.scopus_id

                const.db_manager.insert_scopus_author(
                    base_id=base_id,
                    given_name=author.given_name,
                    surname=author.surname,
                    scopus_id=author.scopus_id)

            const.db_manager.insert_paper_df(author.papers)
