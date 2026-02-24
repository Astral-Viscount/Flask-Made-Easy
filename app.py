from flask import Flask, g
import sqlite3

DATABASE = 'anime.db'
app = Flask(__name__)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

app = Flask(__name__)

@app.route('/')
def home():
    sql = """
        SELECT 
            Anime.image,
            Anime.title,
            Anime.episodes,
            Anime.mal_id
        FROM Anime;
    """
    

    results = query_db(sql)
    return str(results)

@app.route("/anime/<int:id>")
def anime(id):
    sql = """
        SELECT
            Anime.id,
            Anime.image,
            Anime.title,
            Anime.synopsis,
            Anime.episodes,
            GROUP_CONCAT(Genres.name, ', ') AS genres,
            Anime.theme,
            Anime.score,
            Anime.release_date,
            Anime.mal_id
        FROM Anime
        LEFT JOIN AnimeGenres ON Anime.id = AnimeGenres.anime_id
        LEFT JOIN Genres ON AnimeGenres.genre_id = Genres.genre_id
        WHERE Anime.id = ?
        GROUP BY Anime.id;
    """
    result = query_db(sql, (id,), True)
    return str(result)

if __name__ == "__main__":
    app.run(debug=True)