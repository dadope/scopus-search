import pandas as pd
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor
from elsapy.elssearch import ElsSearch

from .. import constants as const
from ..util.commandline_util import log_and_print_if_verbose
from .paper import get_papers_from_doc_list, get_papers_from_author_by_scopus_search, paper_df_from_db_entry


class ScopusAuthor:
    def __init__(self,
                 client: ElsClient,
                 scopus_id: int, given_name: str = None, surname: str = None,
                 verbose: bool = False, ask_user_input: bool = False, output_format: str = const.DEFAULT_NAME_OUTPUT_FORMAT):

        if not scopus_id:
            raise ValueError("Did not receive scopus id")

        self.verbose = verbose
        self.scopus_id = scopus_id
        self.output_format = output_format

        self._els_client = client

        self.in_db = const.db_manager.find_author(scopus_id)
        self._scopus_author = ElsAuthor(author_id=self.scopus_id)

        if not self.in_db:
            if self._scopus_author.read(self._els_client):
                log_and_print_if_verbose(
                    f"first name: {self._scopus_author.first_name}, last name: {self._scopus_author.last_name}",
                    self.verbose)
                given_name, surname = self._scopus_author.first_name, self._scopus_author.last_name

            log_and_print_if_verbose(f"Downloading paper list for {self.scopus_id}, this might take a while", verbose)
            if self._scopus_author.read_docs(self._els_client):
                log_and_print_if_verbose(f"Downloaded paper list!", verbose)
                self.papers = get_papers_from_doc_list(self._scopus_author.doc_list)
            else:
                log_and_print_if_verbose(f"Could not download doc list for {self.scopus_id} from scopus! downloading papers through the search api...", verbose)
                # trying to extract paper information without using the authors index
                self.papers, self.author_name_guesses = (
                    get_papers_from_author_by_scopus_search(self._els_client, self.scopus_id))
                if self.papers.empty:
                    raise ValueError("Could not find author papers, please check your api key permissions")

        else:
            last_updated_paper = const.db_manager.get_last_updated_paper(self.scopus_id)
            if last_updated_paper.empty:
                raise ValueError(f"Could not find any paper in database for author {self.scopus_id}, this should never happen!")
            latest_update_year = int(last_updated_paper["date"][0][:4])
            local_papers = const.db_manager.get_papers_by_scopus_author(self.scopus_id, max_year=latest_update_year).apply(
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

        self.given_name, self.surname = given_name, surname

    def _get_output_key(self) -> str:
        try:
            return self.output_format.format(**dict(
                scopus_id=self.scopus_id,
                given_name=self.given_name,
                surname=self.surname
            ))
        except KeyError:
            return const.DEFAULT_NAME_OUTPUT_FORMAT.format(**dict(given_name=self.given_name, surname=self.surname))

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