#!/usr/bin/env python3
# main.py

from pprint import pprint

from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch

from constants import API_KEY, db_manager

client = ElsClient(API_KEY)

# TODO: automatic author id discovery by name
author_ids = ["22981032300"]

search_results = {}

for author_id in author_ids:
    search_results[author_id] = []

    db_manager.insert_author(author_id, "test", "test") # TODO: replace with author name

    # workaround as we cant use the user api with our current api keys
    search = ElsSearch(query=f"AU-ID({author_id})", index="scopus")
    search.execute(client, get_all=True)

    if search.results:
        search_results[author_id] = search.results
        for paper in search.results:
            title =    paper["dc:title"]
            paper_id = int(paper["dc:identifier"][10:]) # FIXME: removing the leading "SCOPUS:", too hacky
            date =     int(paper["prism:coverDate"][:4]) # FIXME: very hacky date extraction

            db_manager.insert_paper(paper_id, title, date)
            db_manager.insert_written_by(author_id, paper_id)

pprint(search_results)
