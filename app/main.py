import psycopg2
from flask import Flask
import os


# create the Flask app
app = Flask(__name__) 

# Connect to the PostgreSQL database

# create the index route
@app.route('/') 
def index(): 
  return "The API is working!"

# create the data route
@app.route('/get_idw_geojson', methods=['GET'])
def get_idw_geojson():
    # create connection to the DB
    conn = psycopg2.connect(
        host = os.environ.get("DB_HOST"),
        database = os.environ.get("DB_NAME"),
        user = os.environ.get("DB_USER"),
        password = os.environ.get("DB_PASS"),
        port = os.environ.get("DB_PORT"),
    )
    
    # retrieve the data
    with conn.cursor() as cur:
        query = """
        SELECT JSON_BUILD_OBJECT(
            'type', 'FeatureCollection',
            'features', JSON_AGG(
                St_AsGeoJson(idw_pts.*)::json
            )
        )
        FROM idw_pts;
        """
        
        cur.execute(query)
        
        data = cur.fetchall()
    # close the connection
    conn.close()
    
    # Returning the data
    return data [0][0]

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
