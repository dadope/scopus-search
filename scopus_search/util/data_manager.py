from enum import Enum, member
from typing import Callable

from ..models.author import Author


# TODO: implement optional sorting by different values
def _get_json_output(authors: list[Author]):
    return {
        author.key: author.papers.sort_values(by=['date'], ascending=False).apply(
            lambda paper: {
                "scopus_id": paper.scopus_id,
                "title": paper.title,
                "authors": paper.authors,
                "date": paper.date
            }, axis=1
        ).to_list() for author in authors
    }


# TODO: implement
def _get_markdown_output(authors: list[Author]):
    raise NotImplementedError()


def _get_dataframe_output(authors: list[Author]):
    return [author.papers for author in authors]


class OutputFormats(Enum):
    json = member(_get_json_output)
    markdown = member(_get_markdown_output)
    dataframe = member(_get_dataframe_output)


class DataManager:
    def __init__(self, authors: list[Author], output_formatter: Callable | OutputFormats = OutputFormats.json):
        self.authors = authors
        self.output_formatter = output_formatter

    def filter_papers(self,
                      max_year: int = None,
                      min_year: int = None,
                      include_authors: list[int] = [],
                      include_all_authors: list[int] = [],
                      not_include_authors: list[int] = []):
        for author in self.authors:
            author.filter_papers(max_year, min_year, include_authors, include_all_authors, not_include_authors)

    def get_output(self):
        if self.output_formatter in OutputFormats:
            return self.output_formatter.value(self.authors)
        else:
            return self.output_formatter(self.authors)
