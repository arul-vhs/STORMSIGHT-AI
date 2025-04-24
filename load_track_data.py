import pymongo
# from fastkml import kml # No longer needed for parsing
from zipfile import ZipFile # Library to unzip KMZ
import io
import os
from datetime import datetime, timezone # Import timezone for UTC handling
import traceback # To print detailed errors
from lxml import etree # Use LXML for parsing

# --- Configuration ---
MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
DB_NAME = "stormsight_db"
TRACK_COLLECTION_NAME = "track_data" # Use the same collection name
KMZ_FILE_PATH = "data/IO_besttracks_2020-2020.kmz" # <--- Make sure this matches your file
STORM_NAME_FOR_DB = "BESTTRACK_2020" # Give it a unique name in the DB

# --- Function to find the KML file inside KMZ ---
# <<< THIS FUNCTION WAS MISSING IN THE PREVIOUS SNIPPET >>>
def find_kml_in_kmz(kmz_path):
    """
    Reads a KMZ file, finds the first .kml file inside, and returns its content as bytes.
    """
    try:
        with ZipFile(kmz_path, 'r') as kmz:
            # Look for files ending with .kml (case-insensitive)
            kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
            if not kml_files:
                print(f"Error: No .kml file found inside {kmz_path}")
                return None
            # Assume the first KML found is the main one
            kml_filename = kml_files[0]
            print(f"Found KML file: {kml_filename}")
            # Read the KML content as bytes
            kml_content_bytes = kmz.read(kml_filename)
            return kml_content_bytes
    except FileNotFoundError:
        print(f"Error: KMZ file not found at {kmz_path}")
        return None
    except Exception as e:
        print(f"Error reading KMZ file: {e}")
        traceback.print_exc() # Print details on KMZ read errors
        return None
# <<< END OF find_kml_in_kmz FUNCTION >>>


# --- Function to parse KML content using LXML ---
def parse_kml_features(kml_content_bytes):
    """
    Parses KML content (as bytes) using lxml to find Placemarks and extract track data.
    """
    records = []
    if not kml_content_bytes:
        return records

    try:
        # Parse the XML using lxml, trying different encodings if needed
        root = None
        try:
            # Try decoding as UTF-8 first, remove XML declaration
            kml_string = kml_content_bytes.decode('utf-8')
            if kml_string.strip().startswith('<?xml'):
                kml_string = kml_string.split('?>', 1)[-1]
            root = etree.fromstring(kml_string.encode('utf-8')) # Re-encode after cleaning
        except (UnicodeDecodeError, etree.XMLSyntaxError):
             try:
                 # If UTF-8 fails, try latin-1 as a fallback
                 print("Warning: UTF-8 decoding failed, trying latin-1...")
                 kml_string = kml_content_bytes.decode('latin-1')
                 if kml_string.strip().startswith('<?xml'):
                    kml_string = kml_string.split('?>', 1)[-1]
                 root = etree.fromstring(kml_string.encode('latin-1'))
             except Exception as parse_err: # Catch any error during parsing
                 print(f"CRITICAL ERROR: KML file seems invalid XML or uses unexpected encoding. Cannot parse. Details: {parse_err}")
                 return [] # Cannot proceed if parsing fails

        if root is None:
             print("CRITICAL ERROR: Failed to parse KML root element.")
             return []

        # --- Find Placemarks using XPath, handling namespaces ---
        namespaces = {}
        # Try to automatically get namespaces from the root element
        if root.nsmap:
            # Often the KML namespace is the 'default' (no prefix) or uses 'kml'
            if None in root.nsmap:
                namespaces['kml'] = root.nsmap[None]
            elif 'kml' in root.nsmap:
                 namespaces['kml'] = root.nsmap['kml']
            else: # Use the first namespace found as a guess for 'kml'
                first_ns_key = next(iter(root.nsmap))
                namespaces['kml'] = root.nsmap[first_ns_key]
                print(f"Warning: Using detected namespace '{namespaces['kml']}' with prefix 'kml'. Adjust if needed.")

        # If a namespace was found, try searching with it
        placemark_elements = []
        if namespaces:
            try:
                placemark_elements = root.xpath('//kml:Placemark', namespaces=namespaces)
                if not placemark_elements:
                     print(f"Info: No Placemarks found using namespace '{namespaces['kml']}'.")
            except etree.XPathEvalError as xpath_err:
                 print(f"Warning: XPath error with namespace '{namespaces['kml']}'. {xpath_err}")
                 namespaces = {} # Reset namespaces if XPath fails

        # If namespaced search didn't work or no namespace was found, try non-namespaced
        if not placemark_elements:
             print("Info: Trying non-namespaced <Placemark> search...")
             try:
                placemark_elements = root.xpath('//Placemark') # Search for any tag named Placemark
             except etree.XPathEvalError as xpath_err:
                 print(f"Error: Non-namespaced XPath search failed. {xpath_err}")


        print(f"Found {len(placemark_elements)} Placemark elements using lxml.")

        # --- Process each found Placemark element ---
        placemark_count = 0
        print(f"Processing {len(placemark_elements)} placemark elements...")
        for pm_elem in placemark_elements:
            placemark_count += 1
            timestamp = None
            wind_kts = None
            pressure_mb = None
            lat = None
            lon = None

            # 1. Get Coordinates using XPath (try namespaced then non-namespaced)
            coord_text = None
            if namespaces:
                 coord_text = pm_elem.xpath('.//kml:Point/kml:coordinates/text()', namespaces=namespaces)
            if not coord_text: # Try without namespace if namespaced failed or no namespace defined
                coord_text = pm_elem.xpath('.//Point/coordinates/text()')

            if coord_text:
                try:
                    lon_str, lat_str, *_ = coord_text[0].strip().split(',')
                    lon = float(lon_str)
                    lat = float(lat_str)
                except (ValueError, IndexError, TypeError) as e:
                    print(f"Warning: Could not parse coordinates '{coord_text[0]}' in placemark #{placemark_count}. Error: {e}")

            # 2. Get Description Text (try namespaced then non-namespaced)
            desc_text = None
            desc_elements = []
            if namespaces:
                 desc_elements = pm_elem.xpath('.//kml:description/text()', namespaces=namespaces)
            if not desc_elements: # Try without namespace
                 desc_elements = pm_elem.xpath('.//description/text()')
            desc_text = desc_elements[0].strip() if desc_elements else None

            # 3. Parse Data from Description (if description exists)
            if desc_text:
                # --- Extract Timestamp (DTG) ---
                try:
                    if 'DTG </B></td><td>' in desc_text:
                        dtg_part = desc_text.split('DTG </B></td><td>')[1]
                        dtg_val = dtg_part.split('</td>')[0].strip()
                        if dtg_val.endswith('Z') and len(dtg_val) == 11:
                            temp_timestamp_str = dtg_val[:-1]
                            dt_naive = datetime.strptime(temp_timestamp_str, '%Y%m%d%H')
                            timestamp = dt_naive.replace(tzinfo=timezone.utc)
                        else:
                            print(f"Warning: Unexpected DTG format found in placemark #{placemark_count}: {dtg_val}")
                except Exception as e:
                    print(f"Warning: Could not parse DTG from description in placemark #{placemark_count}. Details: {e}")

                # --- Extract Wind Speed (Intensity) ---
                try:
                    if 'Intensity </B></td><td>' in desc_text:
                        intensity_part = desc_text.split('Intensity </B></td><td>')[1]
                        intensity_val = intensity_part.split('</td>')[0].strip()
                        if intensity_val: wind_kts = int(intensity_val)
                except Exception: pass # Ignore errors

                # --- Extract Pressure (MSLP) ---
                try:
                    if 'MSLP </B></td><td>' in desc_text:
                        pressure_part = desc_text.split('MSLP </B></td><td>')[1]
                        if ' mb</td>' in pressure_part: pressure_val = pressure_part.split(' mb</td>')[0].strip()
                        else: pressure_val = pressure_part.split('</td>')[0].strip()
                        if pressure_val and pressure_val.isdigit(): pressure_mb = int(pressure_val)
                except Exception: pass # Ignore errors

            # 4. If we have coordinates and a timestamp, add the record
            if lat is not None and lon is not None and timestamp is not None:
                record = {
                    "storm_id": STORM_NAME_FOR_DB,
                    "timestamp": timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "latitude": lat,
                    "longitude": lon,
                    "location": {"type": "Point", "coordinates": [lon, lat]},
                    "wind_kts": wind_kts,
                    "pressure_mb": pressure_mb
                }
                records.append(record)
            else:
                if lat is not None and lon is not None and timestamp is None:
                    print(f"Warning: Skipping placemark #{placemark_count} - couldn't find valid timestamp (DTG). Coords: ({lat:.2f}, {lon:.2f})")

        # --- End of loop ---

    except etree.XMLSyntaxError as xml_err:
        print(f"\nCRITICAL ERROR: KML file seems invalid XML. Cannot parse. Details: {xml_err}")
        print("----- FULL TRACEBACK -----")
        traceback.print_exc()
        print("----- END TRACEBACK -----")
        return [] # Return empty list on XML error
    except Exception as e:
        print(f"\nERROR during LXML KML parsing: {e}")
        print("----- FULL TRACEBACK -----")
        traceback.print_exc()
        print("----- END TRACEBACK -----")

    print(f"Successfully extracted {len(records)} records using lxml.")
    return records


# --- Main part of the script ---
if __name__ == "__main__":
    print(f"Reading KMZ file: {KMZ_FILE_PATH}")
    # Get KML content using the helper function
    kml_bytes = find_kml_in_kmz(KMZ_FILE_PATH) # Ensure this function is defined above

    if kml_bytes:
        print("Parsing KML content using lxml...")
        # Call the LXML parsing function
        storm_data = parse_kml_features(kml_bytes)

        if storm_data:
            # --- Database Operations ---
            print("Connecting to MongoDB...")
            client = None
            try:
                client = pymongo.MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
                client.admin.command('ismaster') # Verify connection
                print("MongoDB connection successful.")
                db = client[DB_NAME]
                collection = db[TRACK_COLLECTION_NAME]

                print(f"Deleting old data for storm {STORM_NAME_FOR_DB} (if any)...")
                delete_result = collection.delete_many({"storm_id": STORM_NAME_FOR_DB})
                print(f"Deleted {delete_result.deleted_count} old records.")

                print(f"Inserting {len(storm_data)} new records...")
                insert_result = collection.insert_many(storm_data)
                print(f"Inserted {len(insert_result.inserted_ids)} new records.")

                print("Ensuring geospatial index on 'location' field...")
                collection.create_index([("location", pymongo.GEOSPHERE)])
                print("Ensuring time index on 'timestamp' field...")
                collection.create_index([("timestamp", pymongo.ASCENDING)])
                print("\nData loading complete!")

            except pymongo.errors.ServerSelectionTimeoutError as e:
                 print(f"\nERROR: Connection Timeout - Could not connect to MongoDB.")
                 print(f"Please ensure MongoDB server is running. Details: {e}")
            except pymongo.errors.ConnectionFailure as e:
                 print(f"\nERROR: Connection Failure - Could not connect to MongoDB.")
                 print(f"Details: {e}")
            except Exception as e:
                 print(f"\nERROR during database operations: {e}")
                 traceback.print_exc()
            finally:
                if client:
                    client.close()
                    print("MongoDB connection closed.")
            # --- End Database Operations ---
        else:
            print("No data extracted from KML, nothing to insert into database.")
    else:
        print("Could not read KML from KMZ file.")