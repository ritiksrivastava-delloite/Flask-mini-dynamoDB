
from boto3 import resource
import config
from movie_struct import Movie
from boto3.dynamodb.conditions import Key , Attr
import re

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url = ENDPOINT_URL,
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

#create table
def create_table_movie():    
    table = resource.create_table(
        TableName = 'Movie', # Name of the table 
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'S'  
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

movie_table = resource.Table('Movie')

def write_to_movie(movie):
    print(movie.id)
    response = movie_table.put_item(
        Item = {
            "id" : movie.id,
            "title" :movie.title,
            "original_title" : movie.original_title,
            "year": movie.year,
            "date_published": movie.date_published,
            "genre" : movie.genre,
            "duration": movie.duration,
            "country": movie.country,
            "language": movie.language,
            "director": movie.director,
            "writer": movie.writer,
            "production_company": movie.production_company,
            "actors": movie.actors,
            "description": movie.description,
            "avg_vote": movie.avg_vote,
            "votes": movie.votes,
            "budget": movie.budget,
            "usa_gross_income": movie.usa_gross_income,
            "worlwide_gross_income": movie.worlwide_gross_income,
            "metascore": movie.metascore,
            "reviews_from_users": movie.reviews_from_users,
            "reviews_from_critics": movie.reviews_from_critics
        }
    )
    return response

#find movie by id
def find_movie_by_id(id):
    response = movie_table.get_item(
        Key = {
            'id' : id
        }
    )
    return response

#get title by director
def get_title_by_director(director , year1 , year2):
    response = movie_table.scan(
        FilterExpression= Attr('director').contains(director) & Attr('year').between(year1,year2),
        ProjectionExpression = 'title ,id'
    )
    return response

#get movie by language
def movie_review_filter(user_review , language):
    response= movie_table.scan(
        FilterExpression=Attr('reviews_from_users').gt(user_review) & Attr('language').eq(language),
        ProjectionExpression = 'id,title,director,reviews_from_users,#c',
        ExpressionAttributeNames = {'#c': 'language'},
    )
    response['Items'] = sorted(response['Items'],key=lambda x:int(x['reviews_from_users']),reverse=True)
    return response


def load_csv_data():
    is_first = False
    pattern = r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)'
    with open("movies.csv") as f:
        for line in f:
            (
                id,
                title,
                original_title,
                year,
                date_published,
                genre,
                duration,
                country,
                language,
                director,
                writer,
                production_company,
                actors,
                description,
                avg_vote,
                votes,
                budget,
                usa_gross_income,
                worlwide_gross_income,
                metascore,
                reviews_from_users,
                reviews_from_critics
            ) = re.split(pattern, line)
            if is_first==False:
                is_first = True
                continue
            movie = Movie(
                id,
                title,
                original_title,
                year,
                date_published,
                genre,
                duration,
                country,
                language,
                director,
                writer,
                production_company,
                actors,
                description,
                avg_vote,
                votes,
                budget,
                usa_gross_income,
                worlwide_gross_income,
                metascore,
                reviews_from_users,
                reviews_from_critics
            )
            write_to_movie(movie)

def budget_titles_filter(request_data):
    response =movie_table.scan(
    FilterExpression=Attr('country').eq(request_data['country']) & Attr('year').eq(request_data['year']),
    ExpressionAttributeNames = {'#c': 'year'},
    ProjectionExpression = 'id,title,budget,country,director,#c'
    )
    return response