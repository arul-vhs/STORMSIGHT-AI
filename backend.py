import pymongo
from flask import Flask, jsonify, request # Import Flask for web server, jsonify to send data, request to get parameters
from datetime import datetime, timezone, timedelta
import geojson # To format output for maps

# --- Flask App Setup ---
app = Flask(__name__) # Initialize the Flask app

# --- MongoDB Connection ---
MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
DB_NAME = "stormsight_db"
TRACK_COLLECTION_NAME = "track_data"

# Global variable for DB connection (better ways exist, but simple for prototype)
db_client = None
track_collection = None

def get_db_collection():
    """Connects to MongoDB and returns the track collection"""
    global db_client, track_collection
    if track_collection is None:
        try:
            print("Connecting to MongoDB for API...")
            db_client = pymongo.MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=3000)
            db_client.admin.command('ismaster') # Check connection
            db = db_client[DB_NAME]
            track_collection = db[TRACK_COLLECTION_NAME]
            print("MongoDB connection successful for API.")
        except Exception as e:
            print(f"ERROR: Could not connect to MongoDB for API. {e}")
            track_collection = None # Ensure it's None on failure
    return track_collection

# --- API Endpoints ---

@app.route('/') # Default route
def index():
    return "StormSight AI Prototype Backend is running!"

@app.route('/track_data') # Route to get track data
def get_track_data():
    """
    Fetches track data from MongoDB based on optional time parameters.
    Returns data as a GeoJSON FeatureCollection.
    Example query: /track_data?timestamp=2020-11-25T12:00:00Z
                   /track_data?start=2020-11-24T00:00:00Z&end=2020-11-26T00:00:00Z
    """
    collection = get_db_collection()
    if not collection:
        return jsonify({"error": "Database connection failed"}), 500

    query = {} # Start with empty query (get all data)
    features = [] # List to hold GeoJSON features

    # Get query parameters from the URL (if provided)
    timestamp_param = request.args.get('timestamp')
    start_time_param = request.args.get('start')
    end_time_param = request.args.get('end')
    storm_id_param = request.args.get('storm_id', "BESTTRACK_2020") # Default to our loaded storm

    # Filter by storm ID
    query["storm_id"] = storm_id_param

    # --- Time Filtering Logic ---
    if timestamp_param:
        # Find the single point closest IN TIME to the requested timestamp
        # Note: This requires the timestamp index on MongoDB is created
        try:
            target_time = datetime.fromisoformat(timestamp_param.replace('Z', '+00:00'))
            # Find the record immediately before or at the target time
            cursor = collection.find({
                "storm_id": storm_id_param,
                "timestamp": {"$lte": target_time.strftime("%Y-%m-%dT%H:%M:%SZ")}
            }).sort("timestamp", pymongo.DESCENDING).limit(1)
            # We take the *latest* point at or before the requested time
            result = list(cursor) # Execute query and get result list
            if not result: # If no points before, try finding the earliest point overall
                 cursor = collection.find({"storm_id": storm_id_param}).sort("timestamp", pymongo.ASCENDING).limit(1)
                 result = list(cursor)

        except ValueError:
            return jsonify({"error": "Invalid timestamp format. Use ISO format like YYYY-MM-DDTHH:MM:SSZ"}), 400
        except Exception as e:
            print(f"Error querying single timestamp: {e}")
            return jsonify({"error": "Database query failed"}), 500

    elif start_time_param and end_time_param:
        # Find all points within a time range
        try:
            start_time = datetime.fromisoformat(start_time_param.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(end_time_param.replace('Z', '+00:00'))
            query["timestamp"] = {
                "$gte": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "$lte": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            cursor = collection.find(query).sort("timestamp", pymongo.ASCENDING)
            result = list(cursor) # Execute query

        except ValueError:
             return jsonify({"error": "Invalid start/end time format. Use ISO format like YYYY-MM-DDTHH:MM:SSZ"}), 400
        except Exception as e:
            print(f"Error querying time range: {e}")
            return jsonify({"error": "Database query failed"}), 500
    else:
        # No time filter, get all points for the storm
        try:
             cursor = collection.find(query).sort("timestamp", pymongo.ASCENDING)
             result = list(cursor) # Execute query
        except Exception as e:
             print(f"Error querying all data: {e}")
             return jsonify({"error": "Database query failed"}), 500

    # --- Convert MongoDB results to GeoJSON Features ---
    for point_data in result:
        # Basic properties to include
        properties = {
            "timestamp": point_data.get("timestamp"),
            "wind_kts": point_data.get("wind_kts"),
            "pressure_mb": point_data.get("pressure_mb"),
            "storm_id": point_data.get("storm_id"),
            # Add more properties if needed
        }
        # Create GeoJSON Point geometry
        geometry = point_data.get("location") # Use the pre-formatted GeoJSON location

        # Create GeoJSON Feature
        if geometry: # Only create feature if geometry exists
             feature = geojson.Feature(geometry=geometry, properties=properties)
             features.append(feature)

    # Create GeoJSON FeatureCollection
    feature_collection = geojson.FeatureCollection(features)

    # Return the FeatureCollection as JSON
    return jsonify(feature_collection)


# --- Main execution ---
if __name__ == '__main__':
    get_db_collection() # Try initial DB connection when starting
    print("Starting Flask backend server...")
    # Run on port 5000, accessible from any IP on your network (0.0.0.0)
    # Use debug=True only for development (auto-reloads on code changes)
    app.run(host='0.0.0.0', port=5000, debug=True)