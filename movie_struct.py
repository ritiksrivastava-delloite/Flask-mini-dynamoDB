from dataclasses import dataclass
from typing import List

@dataclass
class Movie:
    id : int
    title :str
    original_title : str
    year:int
    date_published: str
    genre : str
    duration: int
    country: str
    language: str
    director: str
    writer: str
    production_company: str
    actors: str
    description: str
    avg_vote: int
    votes: int
    budget: int
    usa_gross_income: int
    worlwide_gross_income: int
    metascore:int
    reviews_from_users:int
    reviews_from_critics:int


#convert the json data into movie 
def return_movie_json(data):
    m =Movie(data["id"],data["title"],data["original_title"],data["year"],data["date_published"],data["genre"],data["duration"],data["country"],data["language"],data["director"],data["writer"],data["production_company"],data["actors"], data["description"],data["avg_vote"],data["votes"],data["budget"],data["usa_gross_income"],data["worlwide_gross_income"],data["metascore"],data["reviews_from_users"],data["reviews_from_critics"])
    return m