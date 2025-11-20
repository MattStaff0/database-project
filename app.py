# backend for the database project
from flask import Flask, render_template, request, redirect, url_for
import mysql.connector
from mysql.connector import Error

app  = Flask(__name__)

#database config
db_config = {
    'user': 'mattstaff',
    'password': 'HaloTopCookies1!',
    'host': 'localhost',
    'database': 'Chinook'
}

# method to start the database connection
def start_connection():
    '''
    Method that starts the connection to the database

    Parameters:
        None
    
    Returns:
        A connector object that is connected to our mysql server to Chinook database
    '''
    return mysql.connector.connect(**db_config)

@app.route('/')
def home():
    conn = start_connection()
    cursor = conn.cursor(dictionary=True)

    # getting the top 10 tracks to display and top 5 artists
    cursor.callproc('GetTopArtists', [5])

    # get the first list returned by stored procedure
    top_artists = []
    for result in cursor.stored_results():
        top_artists = result.fetchall()

    # fetch from the view the last 10 tracks in the database
    # QUERY 1
    query = "SELECT * FROM v_FullTrackInfo ORDER BY TrackId DESC LIMIT 10"
    cursor.execute(query)
    recent_tracks = cursor.fetchall()

    cursor.close()
    conn.close()

    # send the lists to the hmtl template
    return render_template('home.html',
                            top_artists=top_artists,
                            tracks=recent_tracks)

@app.route('/search')
def search():

    # get the search term the user asked for
    query = request.args.get('q') 
    results = []
    
    if query:
        conn = start_connection()
        cursor = conn.cursor(dictionary=True)
        
        # QUERY 2 - 3 Table Join 
        sql = """
            SELECT t.Name AS SongName, a.Title AS AlbumTitle, art.Name AS ArtistName, t.UnitPrice
            FROM Track t
            JOIN Album a ON t.AlbumId = a.AlbumId
            JOIN Artist art ON a.ArtistId = art.ArtistId
            WHERE t.Name LIKE %s OR art.Name LIKE %s OR a.Title LIKE %s
            LIMIT 50
        """

        # execute the sql query using what the user serached for
        wildcard_query = f"%{query}%"
        cursor.execute(sql, (wildcard_query, wildcard_query, wildcard_query))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
    
    return render_template('search.html', results=results)

@app.route('/add', methods=['GET', 'POST'])
def add_transaction():
    conn = start_connection()
    cursor = conn.cursor(dictionary=True)

    message = None
    message_type = None # 'success' or 'error'

    # Handle Form Submission (POST)
    if request.method == 'POST':
        album_id = request.form['album_id']
        name = request.form['name']
        composer = request.form['composer']
        price = request.form['price']
        media_type_id = request.form['media_type_id']
        
        # Run Transaction
        if run_add_track_transaction(album_id, name, composer, price, media_type_id):
            message = 'Success! Track added and linked to Playlist #1.'
            message_type = 'success'
        else:
            message = 'Transaction Failed! Rolled back changes.'
            message_type = 'error'

    # fetch albums to display
    # QUERY 3
    query = """
        SELECT a.AlbumId, a.Title, art.Name as ArtistName 
        FROM Album a 
        JOIN Artist art ON a.ArtistId = art.ArtistId 
        ORDER BY a.Title
    """
    cursor.execute(query)
    albums = cursor.fetchall()
    
    cursor.close()
    conn.close()

    # Send 'message' directly to the template
    return render_template('add_track.html', 
                           albums=albums, 
                           message=message, 
                           message_type=message_type)

# --- UPDATED TRANSACTION FUNCTION ---
def run_add_track_transaction(album_id, name, composer, price, media_type_id):
    conn = start_connection()
    cursor = conn.cursor()
    
    try:
        conn.start_transaction()

        # since we did not use autoincrement for track id we get the max id and add 1 to it
        cursor.execute("SELECT MAX(TrackId) FROM Track")
        row = cursor.fetchone()
        new_track_id = (row[0] + 1) if row and row[0] else 1
        
        # insert the new track into the database
        # QUERY 4
        query_track = """
            INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice)
            VALUES (%s, %s, %s, %s, 1, %s, 200000, 5000000, %s)
        """
        cursor.execute(query_track, (new_track_id, name, album_id, media_type_id, composer, price))
        
        # add to playlist track table for playlist 1
        # #QUERY 5
        query_playlist = "INSERT INTO PlaylistTrack (PlaylistId, TrackId) VALUES (1, %s)"
        cursor.execute(query_playlist, (new_track_id,))
        
        conn.commit()
        return True
        
    except Error as e:
        print(f"❌ Transaction Error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

@app.route('/reports')
def reports():
    conn = start_connection()
    cursor = conn.cursor(dictionary=True)
    
    # QUERY 6 - fetches the top 5 longest albums
    sql_1 = """
        SELECT a.Title, SUM(t.Milliseconds)/60000 AS TotalMinutes
        FROM Album a
        JOIN Track t ON a.AlbumId = t.AlbumId
        GROUP BY a.Title
        ORDER BY TotalMinutes DESC LIMIT 5
    """
    cursor.execute(sql_1)
    longest_albums = cursor.fetchall()

    # QUERY 7 - fetches how many tracks per genre and what are their formats
    sql_2 = """
        SELECT g.Name AS Genre, m.Name AS Format, COUNT(t.TrackId) AS Count
        FROM Track t
        JOIN Genre g ON t.GenreId = g.GenreId
        JOIN MediaType m ON t.MediaTypeId = m.MediaTypeId
        GROUP BY g.Name, m.Name
        ORDER BY Count DESC LIMIT 5
    """
    cursor.execute(sql_2)
    genre_stats = cursor.fetchall()

    # QUERY 8 - fetches all the songs that are longer than the average song length of all songs
    sql_3 = """
        SELECT Name, Milliseconds/1000 AS Seconds 
        FROM Track 
        WHERE Milliseconds > (SELECT AVG(Milliseconds) FROM Track)
        ORDER BY Milliseconds DESC LIMIT 5
    """
    cursor.execute(sql_3)
    long_songs = cursor.fetchall()

    # QUERY 9 - fetches tracks from playlists 
    sql_4 = """
        SELECT t.Name, COUNT(pt.PlaylistId) AS PlaylistCount
        FROM Track t
        JOIN PlaylistTrack pt ON t.TrackId = pt.TrackId
        GROUP BY t.Name
        ORDER BY PlaylistCount DESC LIMIT 5
    """
    cursor.execute(sql_4)
    playlist_stats = cursor.fetchall()

     # QUERY 10 - window function that ranks the tracks 
    try:
        sql_5 = """
            SELECT 
                t.Name, 
                a.Title AS Album,
                t.Milliseconds,
                RANK() OVER (PARTITION BY a.AlbumId ORDER BY t.Milliseconds DESC) as RankInAlbum
            FROM Track t
            JOIN Album a ON t.AlbumId = a.AlbumId
            LIMIT 10
        """
        cursor.execute(sql_5)
        track_rankings = cursor.fetchall()
    except Error:
        track_rankings = []

    # QUERY 11 - fetches all the artists with no albums only songs
    sql_6 = """
        SELECT art.Name 
        FROM Artist art
        LEFT JOIN Album a ON art.ArtistId = a.ArtistId
        WHERE a.AlbumId IS NULL
        LIMIT 5
    """
    cursor.execute(sql_6)
    orphan_artists = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('reports.html', 
                           longest_albums=longest_albums,
                           genre_stats=genre_stats,
                           long_songs=long_songs,
                           playlist_stats=playlist_stats,
                           track_rankings=track_rankings,
                           orphan_artists=orphan_artists)

@app.route('/logs')
def logs():
    conn = start_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Simple Select to get the audit trail
    # This data is populated automatically by the SQL TRIGGER
    query = "SELECT * FROM TrackAuditLog ORDER BY ActionDate DESC"
    cursor.execute(query)
    logs = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('logs.html', logs=logs)

if __name__ == '__main__':
    app.run(debug=True)
