import psycopg2
from flask import Flask
import os


# create the Flask app
app = Flask(__name__) 

# create a general DB to GeoJSON function based on a SQL query
def database_to_geojson_by_query(sql_query):
    # create connection to the DB
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASS"),
        port=os.environ.get("DB_PORT"),
    )
    # retrieve the data
    with conn.cursor() as cur:
        cur.execute(sql_query)
        # fetchall() will return a list of tuples
        data = cur.fetchall()
    # close the connection
    conn.close()

    # Convert query result to GeoJSON format
    features = []
    for row in data:
        # Assuming each row is a GeoJSON feature
        geometry_geojson = row[3]  # Assuming geometry is in the last column
        feature = {
            "type": "Feature",
            "properties": {
                "objectid": row[0],
                "pointid": row[1],
                "cumulative_gdd": row[2]
            },
            "geometry": geometry_geojson
        }
        features.append(feature)

    # Creating GeoJSON FeatureCollection
    geojson_data = {
        "type": "FeatureCollection",
        "features": features
    }

    return geojson_data


# create the index route
@app.route('/') 
def index(): 
    return "The API is working!"


# create the data route
@app.route('/get_soil_moisture_<date>', methods=['GET'])
def get_soil_moisture_geojson(date):
    # call our general function with the provided date
    sm = database_to_geojson_by_query(f"SELECT * FROM samp_soil_moisture_{date}")
    return sm


@app.route('/get_agdd_<countyname>', methods=['GET'])
def get_agdd_idw_geojson(countyname):
    sql_query = f"""
        SELECT agdd.*, ST_AsGeoJSON(agdd.shape)::json
        FROM samp_agdd_idw AS agdd
        JOIN mn_county_1984 AS county 
        ON ST_Contains(county.shape, agdd.shape)
        WHERE county.COUNTYNAME = '{countyname}';
    """

    agdd_idw = database_to_geojson_by_query(sql_query)
    return agdd_idw


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
