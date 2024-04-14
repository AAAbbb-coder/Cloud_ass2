from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from google.cloud import bigquery
import pandas as pd
import requests


# Google BigQuery client
bq_client = bigquery.Client.from_service_account_json('project1-415115-281c33852511.json')

# Elasticsearch connection parameters
URL_ENDPOINT = "https://0739ee45580142a8becbe179bdaae9b4.europe-west9.gcp.elastic-cloud.com:443"
API_KEY = "WHR4djE0NEJFNzM1dV9zT3RTVnI6X3kyeTZQWmVUYnVQc0NEcDBCWjhWUQ=="
INDEX_NAME = "movies_ass2"

client = Elasticsearch(URL_ENDPOINT, api_key=API_KEY)
print(client.info())

# Initialize the Flask application
app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return "Welcome to the Movie Recommendation App Backend!"

@app.route('/search', methods=['GET'])
def search_movies():
    search_query = request.args.get('query', '')
    if search_query:
        es_client = Elasticsearch(URL_ENDPOINT, api_key=API_KEY)
        search_response = es_client.search(
            index=INDEX_NAME,
            body={"query": {"match_phrase_prefix": {"title": {"query": search_query, "max_expansions": 10}}}}
        )
        movie_titles = [hit['_source']['title'] for hit in search_response['hits']['hits']]
        return jsonify(movie_titles)
    return jsonify([])

def similarity_calculator(cold_user_movies, top_5_df):
    # Calculate similarity and weighted score
    def calculate_similarity_and_weight(row):
        common_movies = set(row['top_5_prediction']).intersection(cold_user_movies)
        similarity_count = len(common_movies)
        weighted_score = sum([(5 - row['top_5_prediction'].index(movie)) if movie in common_movies else 0 for movie in row['top_5_prediction']])
        return pd.Series([similarity_count, weighted_score], index=['similarity_count', 'weighted_score'])

    top_5_df[['similarity_count', 'weighted_score']] = top_5_df.apply(calculate_similarity_and_weight, axis=1)
    similarity_scores_df = top_5_df.sort_values(by=['similarity_count', 'weighted_score'], ascending=[False, False])

    # Generate new recommendations excluding movies already seen by the cold user
    def generate_new_recommendations(similarity_scores_df, top_5_df, cold_user_movies):
        top_10_users = similarity_scores_df.head(10)
        all_recommended_movies = top_5_df[top_5_df['userId'].isin(top_10_users['userId'])]['top_5_prediction'].explode().unique()
        new_recommendations = [movie for movie in all_recommended_movies if movie not in cold_user_movies]
        return pd.DataFrame(new_recommendations, columns=['movieId'])

    new_recommendations = generate_new_recommendations(similarity_scores_df, top_5_df, cold_user_movies)
    return similarity_scores_df, new_recommendations

@app.route('/recommendations', methods=['GET'])
def get_recommendations():
    try:
        user_id = request.args.get('userId')  # Capture the userId from the request
        if not user_id:  # Check if userId is provided
            return jsonify({"error": "User ID is required"}), 400

        recommendation_query = f"""
        SELECT * FROM
        ML.RECOMMEND(MODEL `project1-415115.Assignement2.first-MF-model`,
        (
            SELECT DISTINCT userId
            FROM `project1-415115.Assignement2.ratings`
            WHERE userId = {user_id}  # Filter recommendations for the specified user
            LIMIT 5
        ))
        """
        recommendation_result = bq_client.query(recommendation_query).to_dataframe()
        top_5_per_user = recommendation_result.groupby('userId', group_keys=False).apply(lambda x: x.nlargest(7, 'predicted_rating_im_confidence'))
        top_5_movies_per_user = top_5_per_user.groupby('userId')['movieId'].apply(list).reset_index(name='top_5_prediction')
        
        cold_user_movies = [318, 296, 63, 1193, 251]
        _, new_recommendations = similarity_calculator(cold_user_movies, top_5_movies_per_user)
        
        return new_recommendations.to_json()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/poster', methods=['GET'])
def get_movie_posters():
    try:
        # Utiliser directement la clé API
        api_key = 'e7f8b6b7eede8ee0b90df6b4703e292c'

        # Récupérer le titre du film depuis les paramètres de la requête, "Inception" par défaut
        movie_title = request.args.get('title', 'Inception')

        # Construire l'URL pour rechercher le film sur TMDB
        search_url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={movie_title}"
        
        # Envoyer une requête GET à l'API TMDB
        search_response = requests.get(search_url)

        # Vérifier si la recherche a réussi et s'il y a des résultats
        if search_response.status_code == 200 and 'results' in search_response.json() and len(search_response.json()['results']) > 0:
            search_results = search_response.json()['results']
            # Supposer que le premier résultat est le film désiré
            if search_results:
                movieId = search_results[0]['id']
                poster_path = search_results[0]['poster_path']
                # Construire l'URL pour récupérer l'image de l'affiche du film
                poster_url = f"https://image.tmdb.org/t/p/w400{poster_path}"
                return jsonify({"poster_url": poster_url})
            else:
                return jsonify({"error": "Movie not found"}), 404
        else:
            # Retourner un message d'erreur si aucun résultat trouvé ou autre problème
            return jsonify({"error": "Failed to fetch data from TMDB"}), search_response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/load_movies', methods=['GET'])
def load_movies():
    client = bq_client
    query = """
    SELECT m.*, l.imdbId, l.tmdbId 
    FROM `myfirstproject-415115.movie_recommendation_dataset.movies` m 
    INNER JOIN `myfirstproject-415115.movie_recommendation_dataset.links` l 
    ON m.movieId = l.movieId;
    """
    query_job = client.query(query)
    movies_df = query_job.to_dataframe() 
    return movies_df.to_json()

if __name__ == '__main__':
    app.run(debug=True)





