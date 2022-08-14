import time
from functools import wraps
from flask import Flask, request, jsonify, current_app, g 
import movie_service as dynamodb
import user_service as dynamodbUser
import movie_struct
import jwt


app = Flask(__name__)

#need to verify the token
def token_required(f):
    @wraps(f) 
    def decorated(*args, **kwargs):
        token = None
        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']
        
        if  not token:
            return jsonify({'message' : 'Token is missing!'}), 401
        try: 
            jwt.decode(token, "1234", algorithms=["HS256"])     
        except:
            return jsonify({'message' : 'Token is invalid!'}), 401
        return f(*args, **kwargs)
    return decorated

@app.before_request
def before_request():
    g.start = time.time()

@app.after_request
def after_request(response):
    diff = int((time.time() - g.start)*1000)
    response.headers.add("X-TIME-TO-EXECUTE", f"{diff} ms")
    return response

#Route for the user

#to load the csv
@app.route('/loadcsv')
@token_required
def load_csv():
    dynamodb.load_csv_data()
    return 'Table added'

@app.route('/createUserTable')
def user_route():
    dynamodbUser.user_table()
    return "user table created"

@app.route('/register', methods=['POST'])
def add_user():
    data = request.get_json()
    response = dynamodbUser.register_user(data["username"] , data["password"])
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'user successfully added',
            'response': response
        }
    return {  
            'msg': 'Error Occcured',
            'response': response
    }

    

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    response = dynamodbUser.login_user(data["username"] , data["password"])
    print(len(response['Items']))
    if (len(response['Items'])!=0):
        token = jwt.encode({'username': data["username"]},"1234", algorithm="HS256", headers=None, json_encoder=None)
        print(token)
        return {
            'msg': 'token successfully added',
            'token': token
        }
    return {  
            'msg': 'user not found'
    }


#to need to create the table
@app.route('/createTable')
@token_required
def root_route():
    dynamodb.create_table_movie()
    return 'Table created'

@app.route('/movie', methods=['POST'])
@token_required
def add_movie():
    data = request.get_json()
    movie= movie_struct.return_movie_json(data=data)
    response = dynamodb.write_to_movie(movie=movie)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added successfully',
            'response': response
        }
    return {  
        'msg': 'Error occcured',
        'response': response
    }


@app.route('/director', methods=['POST'])
@token_required
def get_movie_by_director():
    data = request.get_json()
    response = dynamodb.get_title_by_director(data["director"], data["year1"], data["year2"])

    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Result Successfully',
            'response': response
        }
    return {  
        'msg': 'Some error occcured',
        'response': response
    }

@app.route('/review', methods=['POST'])
@token_required
def get_movie_by_review():
    data = request.get_json()
    response = dynamodb.movie_review_filter(data["review"], data["language"])
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'result displayed successfully',
            'response': response
        }
    return {  
        'msg': 'Some error occcured',
        'response': response
    }

@app.route('/highBudget', methods=['POST'])
@token_required
def get_movie_by_high_budget():
    data = request.get_json()
    response = dynamodb.budget_titles_filter(data)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'result displayed successfully',
            'response': response
        }
    return {  
        'msg': 'Error Occcured',
        'response': response
    }



if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True)