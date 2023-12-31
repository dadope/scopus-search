import argparse

from elsapy.elsclient import ElsClient

from . import constants
from .models.author import Author
from .util.data_manager import DataManager, OutputFormats

parser = argparse.ArgumentParser(
    prog="scopus-search",
    description="Utility to search and normalize author data found through the Scopus API.")

parser.add_argument("--verbose",            action="store_true",      help="Verbose output")
parser.add_argument("--api_key",            action="store", type=str, help="Sets the API key to use")
parser.add_argument("--filter_by",          action="store", type=str, help="Defines the filters to apply")
parser.add_argument("--output_filetype",    action="store", type=str, help="Defines the output file type")
parser.add_argument("--input_name_format",  action="store", type=str, help="Defines the input format for author names")
parser.add_argument("--output_name_format", action="store", type=str, help="Defines the output format for author names")

parser.add_argument("--max_year", action="store", type=int, help="Filters papers by year (upper bound)")
parser.add_argument("--min_year", action="store", type=int, help="Filters papers by year (lower bound)")

# TODO: add option to resolve author scopus id for name filtering
parser.add_argument("--must_include_authors", action="store", nargs="+", type=int,
                    help="Only papers with one or more of the given authors will be taken into account")
parser.add_argument("--must_not_include_authors", action="store", nargs="+", type=int,
                    help="Only papers without all of the given authors will be taken into account")
parser.add_argument("--must_include_all_authors", action="store", nargs="+", type=int,
                    help="Only papers with all the given authors will be taken into account")

args, author_data = parser.parse_known_args()

api_key = args.api_key or constants.API_KEY
input_name_format = args.input_name_format or constants.DEFAULT_NAME_INPUT_FORMAT
output_name_format = args.output_name_format or constants.DEFAULT_NAME_OUTPUT_FORMAT

if args.output_filetype and (args.output_filetype.lower() in [form.name for form in OutputFormats]):
    output_formatter = OutputFormats.__getitem__(args.output_filetype.lower())
else:
    output_formatter = OutputFormats.__getitem__(constants.DEFAULT_OUTPUT_FILETYPE)


def main():
    if not api_key:
        raise ValueError("Could not find an API key!")

    if not author_data:
        raise ValueError("No author data was input!")

    authors = []
    els_client = ElsClient(api_key)
    els_client.local_dir = constants.project_data_dir
    if all(author.isnumeric() for author in author_data):
        for author_id in author_data:
            authors.append(Author(
                els_client,
                verbose=args.verbose,
                scopus_id=int(author_id),
                output_format=output_name_format
            ))
    else:
        for author_name in author_data:
            authors.append(Author(
                els_client,
                verbose=args.verbose,
                full_name=author_name,
                input_format=input_name_format,
                output_format=output_name_format
            ))

    data_manager = DataManager(authors, output_formatter=output_formatter)

    data_manager.filter_papers(
        args.max_year,
        args.min_year,
        args.must_include_authors,
        args.must_include_all_authors,
        args.must_not_include_authors,
    )

    from pprint import pprint
    pprint(data_manager.get_output())


if __name__ == "__main__":
    main()
