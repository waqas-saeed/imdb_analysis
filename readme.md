## IMDb Analysis ##

This repository analyses IMDb data to find the top 10 highest-rated movies and identifies the most credited persons (actors, directors, or others) for those movies.

### Requirements ###

To run this, you will need:

- `Python 3.10.15`

- Install dependencies listed in `imdb_analysis/requirements.txt`

- Download the following files from [IMDb datasets](https://datasets.imdbws.com):

    - title.basics.tsv
    - title.ratings.tsv
    - name.basics.tsv
    - title.principals.tsv

- Data definition can be found [here](https://developer.imdb.com/non-commercial-datasets/) (not needed to run the analysis)


## Setup ##

1. Install Python 3.10 and verify:

        python --version

2. Create a virtual environment:

        python -m venv my_venv

3. Activate the virtual environment:

    - On macOS/Linux:

            source my_venv/bin/activate

    - On Windows

            my_venv\Scripts\activate

4. Verify Python version:

        python --version

5. Install required packages:

        pip install -r requirements.txt


6. Download [IMDb data files](https://datasets.imdbws.com) (place in the data folder):
    - title.basics.tsv
    - title.ratings.tsv
    - name.basics.tsv
    - title.principals.tsv


## Running the IMDb analysis ##

To run the analysis and find the top 10 movies:

    python movie_rankings.py


### Folder Structure ###
```
└── 📁imdb_analysis
    └── 📁data
        └── name.basics.tsv
        └── title.basics.tsv
        └── title.principals.tsv
        └── title.ratings.tsv
    └── 📁helpers
        └── __init__.py
        └── constants.py
        └── utils.py
    └── 📁schema
        └── __init__.py
        └── titles.py
    └── 📁tests
        └── __init__.py
        └── conftest.py
        └── test_helpers.py
        └── test_movie_rankings.py
    └── .gitignore
    └── movie_rankings.py
    └── readme.md
    └── requirements.in
    └── requirements.txt
    └── setup.py
```
