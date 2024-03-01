import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    version='1.0',
    name='scopus_search',
    python_requires='>=3.11',

    long_description=long_description,
    long_description_content_type="text/markdown",
    description="Utility to search and normalize author data found through the Scopus API",

    url="https://github.com/dadope/scopus-search",

    include_package_data=True,
    packages=setuptools.find_packages(),

    entry_points={
        "console_scripts": ["scopus_search = scopus_search.main:main"]
    },
)
