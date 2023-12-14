import questionary


def log_and_print_if_verbose(message, verbose):
    # TODO: add logger
    if verbose:
        print(message)


def select_from_author_names_list(author_names_list):
    author_names_list.append("Other, input name...")

    index = questionary.select(
        "Select",
        choices=author_names_list
    ).ask()

    if index == len(author_names_list) - 1:
        return input("Input the name of the author: ")

    try:
        name = author_names_list[index].rsplit(" ", 1)
        return name[0], name[1]
    except Exception as e:
        return author_names_list[index]
