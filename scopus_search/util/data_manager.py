from enum import Enum, member
from typing import Callable

from ..models.author import Author


# TODO: implement optional sorting by different values
def _get_json_output(authors: list[Author]):
    return {
        author.key: [
            paper.to_output()
            for paper in sorted(author.papers, key=lambda paper: paper.cover_date[:4], reverse=True)
        ] for author in authors
    }


# TODO: implement
def _get_markdown_output(authors: list[Author]):
    raise NotImplementedError()


class OutputFormats(Enum):
    json = member(_get_json_output)
    markdown = member(_get_markdown_output)


# TODO: implement filtering
class DataManager:
    def __init__(self, authors: list[Author], output_formatter: Callable | OutputFormats = OutputFormats.json):
        self.authors = authors
        self.output_formatter = output_formatter

    def get_output(self):
        if self.output_formatter in OutputFormats:
            return self.output_formatter.value(self.authors)
        else:
            return self.output_formatter(self.authors)
