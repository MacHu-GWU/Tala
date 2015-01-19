##encoding=utf8

from __future__ import print_function
from tala.fields import *
from tala.engine import *
import pandas as pd

"""Define Schema"""
movie_schema = Schema("movie",
    Field("movie_id", Searchable_UUID, primary_key=True),
    Field("title", Searchable_TEXT),
    Field("year", Searchable_INTEGER),
    Field("length", Searchable_INTEGER),
    Field("rating", Searchable_REAL),
    Field("votes", Searchable_INTEGER),
    Field("genres", Searchable_KEYWORD),
    )

engine = SearchEngine("movies.db", movie_schema)

def add_data():
    """extract documents from raw data, and add documents to engine
    """
    df = pd.read_csv("movies.tab", sep="\t")
    documents = list()
    for index, row in df.iterrows():
        document = dict()
        document["movie_id"] = str(index)
        document["title"] = row["title"]
        document["year"] = row["year"]
        document["length"] = row["length"]
        document["votes"] = row["votes"]
        genres = list()
        if row["Action"]:
            genres.append("Action")
        if row["Animation"]:
            genres.append("Animation")
        if row["Comedy"]:
            genres.append("Comedy")
        if row["Drama"]:
            genres.append("Drama")
        if row["Documentary"]:
            genres.append("Documentary")
        if row["Romance"]:
            genres.append("Romance")
        if row["Short"]:
            genres.append("Short")
        document["genres"] = "&".join(genres)
        documents.append(document)
    
    engine.add_all(documents) # add data to engine

def search():
    query = engine.create_query()
    query.add(query.query_between("year", 1993, 2000)) # year between 1993, 2000
    query.add(query.query_contains("genres", "Drama")) # genres contains Drama
    for document in engine.search_document(query):
        print(document)

add_data() # step 1 add data        
search() # step 2 do query