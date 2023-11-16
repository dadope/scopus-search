# Scopus-search

> Package and commandline tool that automatically downloads and formats author-paper information using the scopus api

## Features
> work in progress

## Installation / Setup
to install the program, run:
```
python3.11 -m pip install -r requirements.txt
python3.11 -m pip install .
```
_please note that python 3.11 or greater is required to run the project_

To not have to constantly input your api key through commandline arguments, store your api key in the  config file under `~/.scopus_search/config.json`

## Usage

### commandline example

```commandline
scopus_search --api_key "<key>" \
    --input_name_format "{given_name}, {surname}" "Terence, Tao" "Ben Joseph, Green"
```

### package

> work in progress

## Notice of Non-Affiliation and Disclaimer

This project is not affiliated, associated, authorized, endorsed by, or in any way officially connected with
Scopus / Elsevier, or any of its subsidiaries or its affiliates.

The names Scopus and Elsevier as well as related names, marks, emblems and images are registered trademarks of their
respective owners.