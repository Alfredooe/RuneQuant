import requests
import time
import logging
import json
import re
import os
from datetime import datetime
import pytz
from bs4 import BeautifulSoup
from pymongo import MongoClient, ASCENDING

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('grandexchange')

# Define the columns we want to display
WANTED_COLUMNS = [
    "item_name",
    "high_price_1h",
    "high_price_5m",
    "high_volume_1h",
    "high_volume_5m",
    "low_price_1h",
    "low_price_5m",
    "low_volume_1h",
    "low_volume_5m",
    "player_count",
    "timestampElapsed",
    "gst"
]

def get_mongo_client():
    """Create a connection to MongoDB."""
    retries = 5
    while retries > 0:
        try:
            mongo_uri = os.environ.get('MONGO_URI', 'mongodb://mongodb:27017/')
            client = MongoClient(mongo_uri)
            # Ping the server to check connection
            client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return client
        except Exception as e:
            retries -= 1
            logger.warning(f"Could not connect to MongoDB, retrying... ({retries} attempts left): {e}")
            time.sleep(5)
    
    logger.error("Failed to connect to MongoDB after multiple attempts")
    return None

def initialize_database():
    """Initialize the MongoDB database."""
    client = get_mongo_client()
    if not client:
        logger.error("Could not initialize database - connection failed")
        return False
    
    try:
        # Get database and collection
        db_name = os.environ.get('MONGO_DB', 'runequant')
        db = client[db_name]
        
        # Create indexes for better query performance
        price_coll = db.price_data
        price_coll.create_index([("timestamp", ASCENDING)])
        price_coll.create_index([("item_id", ASCENDING)])
        
        logger.info("MongoDB database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing MongoDB: {e}")
        return False

def load_item_mapping():
    """Load the mapping from item IDs to names from mapping.json"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_path = os.path.join(script_dir, 'mapping.json')
        
        with open(mapping_path, 'r') as f:
            mapping_data = json.load(f)
        
        # Create a dictionary mapping item ID to item name
        item_id_to_name = {}
        for item in mapping_data:
            if 'id' in item and 'name' in item:
                item_id_to_name[str(item['id'])] = item['name']
        
        logger.info(f"Loaded {len(item_id_to_name)} item mappings from mapping.json")
        return item_id_to_name
    except Exception as e:
        logger.error(f"Error loading item mapping: {e}")
        return {}  # Return empty dict on error

def get_player_count():
    """Fetch the current player count from the OSRS website."""
    try:
        # Fetch the OSRS homepage
        response = requests.get("https://oldschool.runescape.com/", timeout=10)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the player count element
        player_count_elem = soup.select_one("p.player-count")
        if not player_count_elem:
            logger.warning("Player count element not found on OSRS website")
            return "N/A"
        
        # Extract the number from the text
        player_count_text = player_count_elem.text
        match = re.search(r'(\d{1,3}(?:,\d{3})*)', player_count_text)
        if match:
            # Remove commas from the number and convert to integer
            player_count = match.group(1).replace(',', '')
            logger.info(f"Current player count: {player_count}")
            return player_count
        else:
            logger.warning(f"Could not extract player count from text: {player_count_text}")
            return "N/A"
            
    except Exception as e:
        logger.error(f"Error fetching player count: {e}")
        return "N/A"

def save_price_data_to_mongo(formatted_data, timestamp):
    """Save the price data to MongoDB."""
    client = get_mongo_client()
    if not client:
        logger.error("Could not save price data - MongoDB connection failed")
        return False
    
    try:
        # Get database and collection
        db_name = os.environ.get('MONGO_DB', 'runequant')
        db = client[db_name]
        price_coll = db.price_data
        
        # Prepare documents for insertion
        documents = []
        for item_id, item_data in formatted_data.items():
            # Create document with all fields
            document = {
                "timestamp": timestamp,
                "item_id": item_id,
                "collection_time": datetime.now()
            }
            
            # Add all data fields
            for key, value in item_data.items():
                document[key] = value
                
            documents.append(document)
        
        # Insert documents in batch
        if documents:
            result = price_coll.insert_many(documents)
            logger.info(f"Saved {len(result.inserted_ids)} price records to MongoDB")
            return True
        else:
            logger.warning("No documents to insert")
            return False
        
    except Exception as e:
        logger.error(f"Error saving price data to MongoDB: {e}")
        return False
    
    finally:
        client.close()

def fetch_prices():
    base_url = "https://prices.runescape.wiki/api/v1/osrs"
    
    # Load the item ID to name mapping
    item_id_to_name = load_item_mapping()
    
    # Get player count from OSRS website
    player_count = get_player_count()
    
    # Get 5-minute data
    try:
        response_5m = requests.get(f"{base_url}/5m")
        response_5m.raise_for_status()
        data_5m = response_5m.json()
        logger.debug(f"Fetched 5m prices for timestamp: {data_5m.get('timestamp')}")
    except Exception as e:
        logger.error(f"Error fetching 5m prices: {e}")
        data_5m = {"data": {}, "timestamp": 0}
        
    # Get 1-hour data
    try:
        response_1h = requests.get(f"{base_url}/1h")
        response_1h.raise_for_status()
        data_1h = response_1h.json()
        logger.debug(f"Fetched 1h prices for timestamp: {data_1h.get('timestamp')}")
    except Exception as e:
        logger.error(f"Error fetching 1h prices: {e}")
        data_1h = {"data": {}, "timestamp": 0}
    
    # Current timestamp for elapsed time calculation
    current_timestamp = int(time.time())
    timestamp_elapsed = current_timestamp - data_5m.get('timestamp', current_timestamp)
    
    # Get GST time
    gst_time = datetime.now(pytz.timezone('GMT')).strftime('%Y-%m-%d %H:%M:%S GMT')
    
    # Format the data for all items
    formatted_data = {}
    
    # Combine data from both endpoints
    for item_id, data_5m_values in data_5m.get('data', {}).items():
        if item_id not in formatted_data:
            formatted_data[item_id] = {}
            
        # Add item name from mapping if available
        formatted_data[item_id]['item_name'] = item_id_to_name.get(item_id, f"Unknown Item ({item_id})")
            
        # Add 5m data
        formatted_data[item_id]['high_price_5m'] = data_5m_values.get('avgHighPrice')
        formatted_data[item_id]['high_volume_5m'] = data_5m_values.get('highPriceVolume')
        formatted_data[item_id]['low_price_5m'] = data_5m_values.get('avgLowPrice')
        formatted_data[item_id]['low_volume_5m'] = data_5m_values.get('lowPriceVolume')
        
        # Add common data
        formatted_data[item_id]['player_count'] = player_count
        formatted_data[item_id]['timestampElapsed'] = timestamp_elapsed
        formatted_data[item_id]['gst'] = gst_time
    
    # Add 1h data
    for item_id, data_1h_values in data_1h.get('data', {}).items():
        if item_id not in formatted_data:
            formatted_data[item_id] = {
                'item_name': item_id_to_name.get(item_id, f"Unknown Item ({item_id})"),
                'high_price_5m': None,
                'high_volume_5m': None,
                'low_price_5m': None,
                'low_volume_5m': None,
                'player_count': player_count,
                'timestampElapsed': timestamp_elapsed,
                'gst': gst_time
            }
            
        # Add 1h data
        formatted_data[item_id]['high_price_1h'] = data_1h_values.get('avgHighPrice')
        formatted_data[item_id]['high_volume_1h'] = data_1h_values.get('highPriceVolume')
        formatted_data[item_id]['low_price_1h'] = data_1h_values.get('avgLowPrice')
        formatted_data[item_id]['low_volume_1h'] = data_1h_values.get('lowPriceVolume')
    
    # Save price data to MongoDB with mapped item names
    save_price_data_to_mongo(formatted_data, data_5m.get('timestamp'))
    
    # Log some sample data (first 5 items)
    sample_items = list(formatted_data.keys())[:5]
    for item_id in sample_items:
        item_data = formatted_data[item_id]
        logger.info(f"Item ID: {item_id} - Name: {item_data.get('item_name', 'Unknown')}")
        for column in WANTED_COLUMNS[1:]:  # Skip item_name as we already logged it
            logger.info(f"  {column}: {item_data.get(column, 'N/A')}")
        logger.info("---")
    
    # Log summary
    logger.info(f"Total items processed: {len(formatted_data)}")
    
    return formatted_data

def main():
    logger.info("Starting Grand Exchange price tracker")
    
    # Initialize the database
    if not initialize_database():
        logger.error("Database initialization failed, exiting...")
        return
    
    # Collect data at regular intervals
    while True:
        fetch_prices()
        time.sleep(300)  # Fetch every 5 minutes

if __name__ == "__main__":
    main()
