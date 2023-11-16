from typing import Callable


class Paper:
    def __init__(self, scopus_id: int, authors: list[int], title: str, cover_date: str):
        self.cover_date = cover_date
        self.scopus_id = scopus_id
        self.authors = authors
        self.title = title

    def __str__(self):
        return str(self.to_output())

    def to_output(self, custom_formatter: Callable | str = None):
        data_dict = dict(
            cover_date=self.cover_date,
            scopus_id=self.scopus_id,
            authors=self.authors,
            title=self.title
        )

        if not custom_formatter:
            return data_dict
        elif type(custom_formatter) is str:
            return custom_formatter.format(**data_dict)
        else:
            try:
                return custom_formatter(self)
            except Exception as e:
                raise ValueError("Could not properly convert paper to output format! ran into error: " + str(e))


def get_paper_from_search_entry(search_entry: dict) -> Paper:
    return Paper(
        int(search_entry["dc:identifier"].replace("SCOPUS_ID:", "")),
        [int(author["authid"]) for author in search_entry["authors"]["author"]],
        search_entry["dc:title"],
        search_entry["prism:coverDate"]
    )
