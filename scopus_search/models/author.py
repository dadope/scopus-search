import pandas as pd
from parse import parse
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor
from elsapy.elssearch import ElsSearch

from .. import constants as const
from .paper import paper_df_from_db_entry, get_papers_from_doc_list, get_papers_from_author_by_scopus_search, \
    save_paper_to_db
from ..util.commandline_util import log_and_print_if_verbose, select_from_author_names_list


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
                 output_format: str = const.DEFAULT_NAME_OUTPUT_FORMAT,
                 verbose: bool = False):

        if not (scopus_id or full_name or (given_name and surname)):
            raise ValueError("Did not receive scopus id or full name or first and last names of the author")

        self.verbose = verbose
        self._els_client = client
        self.output_format = output_format

        self.author_name_guesses = None

        if full_name and not (given_name and surname):
            self.given_name, self.surname = _extract_names_from_full_name(full_name, input_format)

        self.scopus_id = scopus_id or self._get_scopus_id_by_name()
        self._scopus_author = ElsAuthor(author_id=self.scopus_id)

        if const.db_manager.find_author(self.scopus_id):
            log_and_print_if_verbose(f"Found: {self.scopus_id} in db, loading papers", verbose)

            # we sort the papers, so the first entry is always the latest
            latest_update_year = int(const.db_manager.get_papers_by_author(self.scopus_id)["date"][0][:4])
            local_papers = const.db_manager.get_papers_by_author(self.scopus_id, max_year=latest_update_year).apply(
                paper_df_from_db_entry, axis=1)
            new_papers, _ = get_papers_from_author_by_scopus_search(
                self._els_client,
                self.scopus_id,
                max_year=latest_update_year
            )

            log_and_print_if_verbose(
                f"Loaded {len(local_papers)} from database, updated / downloaded {len(new_papers)} papers from scopus",
                verbose)

            self.papers = pd.concat([new_papers, local_papers], ignore_index=True)

        else:
            if self._scopus_author.read_docs(self._els_client):
                log_and_print_if_verbose(
                    f"Downloading paper list for {self.scopus_id}... this might take a while", verbose)
                self.papers = get_papers_from_doc_list(self._scopus_author.doc_list)
            else:
                log_and_print_if_verbose(
                    f"Could not download doc list for {self.scopus_id} from scopus! downloading papers through the search api...",
                    verbose)
                # trying to extract paper information without using the authors index
                self.papers, self.author_name_guesses = (
                    get_papers_from_author_by_scopus_search(self._els_client, self.scopus_id))
                if self.papers.empty:
                    raise ValueError("Could not find author papers, please check your api key permissions")

        if not (given_name and surname):
            self.given_name, self.surname = self._get_author_name_by_scopus_id()

        self.key = self._get_key()
        self._save_to_db()

    def _save_to_db(self):
        log_and_print_if_verbose(f"Saving author: {self.scopus_id} and papers to database..", self.verbose)
        const.db_manager.insert_author(self.scopus_id, self.given_name, self.surname)
        self.papers.apply(save_paper_to_db, axis=1)

    def _get_key(self) -> str:
        try:
            return self.output_format.format(**dict(
                scopus_id=self.scopus_id,
                given_name=self.given_name,
                surname=self.surname
            ))
        except KeyError:
            return const.DEFAULT_NAME_OUTPUT_FORMAT.format(**dict(given_name=self.given_name, surname=self.surname))

    def _get_scopus_id_by_name(self):
        log_and_print_if_verbose(
            f"Finding scopus_id by first ({self.given_name}) and last ({self.surname}) name",
            self.verbose)
        author_info = const.db_manager.get_author(given_name=self.given_name, surname=self.surname)
        if not author_info.empty:
            return int(author_info.scopus_id[0])

        query = f"AUTHFIRST({self.given_name}) AND AUTHLASTNAME({self.surname})"
        search = ElsSearch(query, index="author")
        search.execute(self._els_client)

        if not search.results or "error" in search.results[0].keys():
            raise ValueError("Could not find author scopus id, please check your api key permissions")

        log_and_print_if_verbose(
            f"Found {len(search.results)} entries, selecting best matching user...", self.verbose)

        # TODO: consider multiple search entries
        search_entry = search.results[0]

        return int(search_entry["dc:identifier"].replace("AUTHOR_ID:", ""))

    def _get_author_name_by_scopus_id(self):
        log_and_print_if_verbose(f"Finding author first and last name by scopus_id ({self.scopus_id})", self.verbose)
        author_info = const.db_manager.get_author(author_scopus_id=self.scopus_id)
        if not author_info.empty:
            return author_info.given_name[0], author_info.surname[0]

        if not self._scopus_author.read(self._els_client):
            log_and_print_if_verbose("Could not find author names, using empty placeholder", self.verbose)

            if self.author_name_guesses:
                return select_from_author_names_list(self.author_name_guesses)
            else:
                return "", ""

        log_and_print_if_verbose(
            f"first name: {self._scopus_author.first_name}, last name: {self._scopus_author.last_name}", self.verbose)
        return self._scopus_author.first_name, self._scopus_author.last_name

    def filter_papers(self,
                      max_year: int = None,
                      min_year: int = None,
                      include_authors: list[int] = [],
                      include_all_authors: list[int] = [],
                      not_include_authors: list[int] = []):

        if max_year or min_year or include_authors or include_all_authors or not_include_authors:
            log_and_print_if_verbose("Filtering papers...", self.verbose)

        if max_year:
            self.papers = self.papers.loc[self.papers.date.map(lambda date: int(date[:4])) < max_year]

        if min_year:
            self.papers = self.papers.loc[self.papers.date.map(lambda date: int(date[:4])) > min_year]

        if not_include_authors:
            self.papers = self.papers[self.papers.authors.apply(
                lambda authors: not any(author in authors for author in not_include_authors))]

        if include_authors:
            self.papers = self.papers[self.papers.authors.apply(
                lambda authors: any(author in authors for author in include_authors))]

        if include_all_authors:
            self.papers = self.papers[self.papers.authors.apply(
                lambda authors: set(include_all_authors) <= set(authors))]

        return self.papers
