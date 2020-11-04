from flask import current_app, g
from werkzeug.local import LocalProxy

from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.write_concern import WriteConcern
from pymongo.errors import DuplicateKeyError, OperationFailure
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.read_concern import ReadConcern


def get_db():
    db = getattr(g, "_database", None)
    MFLIX_DB_URI = current_app.config["MFLIX_DB_URI"]
    MFLIX_DB_NAME = current_app.config["MFLIX_NS"]
    if db is None:
        db = g._database = MongoClient(
        MFLIX_DB_URI,maxPoolSize=50,
        wTimeoutMS=2500
        )[MFLIX_DB_NAME]
    return db
db = LocalProxy(get_db)


def get_movies_by_country(countries):
    try:
        return list(db.movies.find({"countries":{ "$in": countries} }, {"title":1 }))
    except Exception as e:
        return e


def get_movies_faceted(filters, page, movies_per_page):
    sort_key = "tomatoes.viewer.numReviews"
    pipeline = []
    if "cast" in filters:
        pipeline.extend([{
            "$match": {"cast": {"$in": filters.get("cast")}}
        }, {
            "$sort": {sort_key: DESCENDING}
        }])
    else:
        raise AssertionError("No filters to pass to faceted search!")

    counting = pipeline[:]
    count_stage = {"$count": "count"}
    counting.append(count_stage)

    skip_stage = {"$skip": movies_per_page * page}
    limit_stage = {"$limit": movies_per_page}
    facet_stage = {
        "$facet": {
            "runtime": [{
                "$bucket": {
                    "groupBy": "$runtime",
                    "boundaries": [0, 60, 90, 120, 180],
                    "default": "other",
                    "output": {
                        "count": {"$sum": 1}
                    }
                }
            }],
            "rating": [{
                "$bucket": {
                    "groupBy": "$metacritic",
                    "boundaries": [0, 50, 70, 90, 100],
                    "default": "other",
                    "output": {
                        "count": {"$sum": 1}
                    }
                }
            }],
            "movies": [{
                "$addFields": {
                    "title": "$title"
                }
            }]
        }
    }
    pipeline.append(skip_stage)
    pipeline.append(limit_stage)
    pipeline.append(facet_stage)

    try:
        movies = list(db.movies.aggregate(pipeline, allowDiskUse=True))[0]
        count = list(db.movies.aggregate(counting, allowDiskUse=True))[
            0].get("count")
        return (movies, count)
    except OperationFailure:
        raise OperationFailure(
            "Results too large to sort, be more restrictive in filter")


def build_query_sort_project(filters):
    """
    Builds the `query` predicate, `sort` and `projection` attributes for a given
    filters dictionary.
    """
    query = {}
    sort = [("tomatoes.viewer.numReviews", DESCENDING), ("_id", ASCENDING)]
    project = None
    if filters:
        if "text" in filters:
            query = {"$text": {"$search": filters["text"]}}
            meta_score = {"$meta": "textScore"}
            sort = [("score", meta_score)]
            project = {"score": meta_score}
        elif "cast" in filters:
            query = {"cast": {"$in": filters["cast"]}}
        elif "genres" in filters:
           
            query = {"genres":{"$in":filters["genres"]}}

    return query, sort, project


def get_movies(filters, page, movies_per_page):
    
    query, sort, project = build_query_sort_project(filters)
    if project:
        cursor = db.movies.find(query, project).sort(sort)
    else:
        cursor = db.movies.find(query).sort(sort)

    total_num_movies = 0
    if page == 0:
        total_num_movies = db.movies.count_documents(query)
    
    if page==0:
        movies = cursor.limit(movies_per_page)
    else:
        movies=cursor.skip(int(page)*int(movies_per_page)).limit(movies_per_page)

    return (list(movies), total_num_movies)


def get_movie(id):
    try:
        pipeline = [
            {
                "$match": {
                    "_id": ObjectId(id)
                }
            },
             {
                "$lookup": {
                    "from": 'comments',
                    "let": { 'id': '$_id' },
                    "pipeline": [
                        { '$match':
                            { '$expr': { '$eq': [ '$movie_id', '$$id' ] } }
                        }
                    ],
                    "as": 'comments'
                }
            }
        ]

        movie = db.movies.aggregate(pipeline).next()
        return movie
     
    except (StopIteration) as e:

        
        return None

    except InvalidId as e:
        return None


def get_all_genres():
    """
    Returns list of all genres in the database.
    """
    return list(db.movies.aggregate([
        {"$unwind": "$genres"},
        {"$group": {"_id": None, "genres": {"$addToSet": "$genres"}}}
    ]))[0]["genres"]




def add_comment(movie_id, user, comment, date):
    
    comment_doc = {
        "name": user.name,
        "email": user.email,
        "movie_id": ObjectId(movie_id),
        "text": comment,
        "date": date
     }
    return db.comments.insert_one(comment_doc)


def update_comment(comment_id, user_email, text, date):
    response = db.comments.update_one({"_id": ObjectId(comment_id), "email": user_email},
        {"$set": {"text": text, "date": date}}
    )

    return response


def delete_comment(comment_id, user_email):
    response = db.comments.delete_one( { "_id":ObjectId(comment_id),"email": user_email })
    return response



def get_user(email):
    return db.users.find_one({ "email": email })


def add_user(name, email, hashedpw):
    try:
        db.users.insert_one({
            "name": name,
            "email": email,
            "password":hashedpw
        })
        return {"success": True}
    except DuplicateKeyError:
        return {"error": "A user with the given email already exists."}


def login_user(email, jwt):
    """
    Given an email and JWT, logs in a user by updating the JWT corresponding
    with that user's email in the `sessions` collection.

    In `sessions`, each user's email is stored in a field called "user_id".
    """ 
    try:
        db.sessions.update_one(
            { "user_id": email},
            { "$set": {"jwt": jwt } },
            upsert=True
        )
        return {"success": True}
    except Exception as e:
        return {"error": e}


def logout_user(email):
    """
    Given a user's email, logs out that user by deleting their corresponding
    entry in the `sessions` collection.

    In `sessions`, each user's email is stored in a field called "user_id".
    """
    try:
        db.sessions.delete_one({ "user_id":email })
        return {"success": True}
    except Exception as e:
        return {"error": e}


def get_user_session(email):
    """
    Given a user's email, finds that user's session in `sessions`.

    In `sessions`, each user's email is stored in a field called "user_id".
    """
    try:
        return db.sessions.find_one({ "user_id": email })
    except Exception as e:
        return {"error": e}


def delete_user(email):
    """
    Given a user's email, deletes a user from the `users` collection and deletes
    that user's session from the `sessions` collection.
    """
    try:
        db.sessions.delete_one({ "user_id": email })
        db.users.delete_one({ "email": email })
        if get_user(email) is None:
            return {"success": True}
        else:
            raise ValueError("Deletion unsuccessful")
    except Exception as e:
        return {"error": e}


def update_prefs(email, prefs):
    """
    Given a user's email and a dictionary of preferences, update that user's
    preferences.
    """
    prefs = {} if prefs is None else prefs
    try:
        response = db.users.update_one(
            {"email": email}, {"$set": {"preferences": prefs}})
        if response.matched_count == 0:
            return {'error': 'no user found'}
        else:
            return response
    except Exception as e:
        return {'error': str(e)}


def most_active_commenters():
    """
    Returns a list of the top 20 most frequent commenters.
    """

    pipeline = [
          {
            "$group": {
                "_id": "$email",
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 20
        }
    ]

    rc = db.comments.read_concern # you may want to change this read concern!
    comments = db.comments.with_options(read_concern=rc)
    result = comments.aggregate(pipeline)
    return list(result)


def make_admin(email):
    """
    Supplied method
    Flags the supplied user an an admin
    """
    db.users.update_one({"email": email}, {"$set": {"isAdmin": True}})


def get_configuration():
    """
    Returns the following information configured for this client:

    - max connection pool size
    - write concern
    - database user role
    """

    try:
        role_info = db.command({'connectionStatus': 1}).get('authInfo').get(
            'authenticatedUserRoles')[0]
        return (db.client.max_pool_size, db.client.write_concern, role_info)
    except IndexError:
        return (db.client.max_pool_size, db.client.write_concern, {})
