import questionary


def log_and_print_if_verbose(message, verbose):
    # TODO: add logger
    if verbose:
        print(message)


def select_from_author_names_list(author_names_list):
    author_names_list = list(author_names_list)
    author_names_list.append("Other, input name...")


    choice = questionary.select(
        "Select",
        choices=author_names_list
    ).ask()

    if choice == "Other, input name...":
        choice = input("Input the name of the author: ")

    try:
        name = choice.rsplit(" ", 1)
        return name[0], name[1]
    except Exception as e:
        return choice, " "