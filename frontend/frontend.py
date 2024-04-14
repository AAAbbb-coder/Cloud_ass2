import streamlit as st
import requests

BASE_URL = "http://127.0.0.1:5000"  # Update this with your backend URL

def search_movies(query):
    """Send a search request to the Flask server and display the results."""
    response = requests.get(f"{BASE_URL}/search", params={'query': query})
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to fetch search results.")
        return []

def display_search_results():
    """Create a user interface for searching movies."""
    st.header("Search for Movies")
    query = st.text_input("Enter the movie title to search:", key="search_box")
    if query:
        results = search_movies(query)
        if results:
            for movie_title in results:
                if st.button(movie_title):
                    display_and_get_movie_posters(movie_title)

def display_and_get_movie_posters(movie_title):
    """Display movie poster for the given movie title."""
    st.header(f"Poster for {movie_title}")
    response = requests.get(f"{BASE_URL}/poster", params={'title': movie_title})
    if response.status_code == 200:
        data = response.json()
        poster_url = data.get('poster_url')
        if poster_url:
            st.image(poster_url, caption=f"Poster for {movie_title}")
        else:
            st.warning("No poster available for this movie.")
    else:
        st.error("Failed to fetch movie poster.")

def main():
    """Main function to run the Streamlit application."""
    st.title("Movie Management Application")
    display_search_results()

if __name__ == "__main__":
    main()
