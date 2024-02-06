import questionary


def log_and_print_if_verbose(message, verbose):
    # TODO: add logger
    if verbose:
        print(message)


def select_from_author_names_list(author_names_list):
    author_names_list = list(author_names_list)
    author_names_list.append("Other, input name...")

    choice = questionary.select(
        "Please select one of the following authors, or select other to input the name",
        choices=author_names_list
    ).ask()

    if choice == "Other, input name...":
        choice = input("Input the name of the author: ")
    else:
        # removing the author count
        choice = choice.split(" ", 1)[1]

    try:
        name = choice.rsplit(" ", 1)
        return name[0], name[1]
    except Exception as e:
        return choice, " "


def select_from_author_search_entries(author_search_entries):
    choice = questionary.checkbox(
        "Please select the authors you want to include",
        choices=author_search_entries
    ).ask()
