import pandas as pd
import os
import logging
from pymongo import MongoClient
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('zamorak')

def get_mongo_client():
    """Create a connection to MongoDB."""
    retries = 5
    while retries > 0:
        try:
            mongo_uri = os.environ.get('MONGO_URI', 'mongodb://root:example@mongo:27017/')
            client = MongoClient(mongo_uri)
            # Ping the server to check connection
            client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return client
        except Exception as e:
            retries -= 1
            logger.warning(f"Could not connect to MongoDB, retrying... ({retries} attempts left): {e}")
            if retries > 0:
                import time
                time.sleep(5)
    
    logger.error("Failed to connect to MongoDB after multiple attempts")
    return None

def get_price_data(days=7, item_id=None):
    """
    Retrieve price data from MongoDB and convert to DataFrame.
    
    Args:
        days (int): Number of days of data to retrieve
        item_id (str, optional): Specific item ID to filter by
    
    Returns:
        pd.DataFrame: DataFrame containing the price data
    """
    client = get_mongo_client()
    if not client:
        logger.error("Could not retrieve price data - MongoDB connection failed")
        return pd.DataFrame()
    
    try:
        # Get database and collection
        db_name = os.environ.get('MONGO_DB', 'runequant')
        db = client[db_name]
        price_coll = db.price_data
        
        # Build query with date filter
        query = {}
        if days:
            cutoff_date = datetime.now() - timedelta(days=days)
            query['collection_time'] = {'$gte': cutoff_date}
        
        # Add item_id filter if provided
        if item_id:
            query['item_id'] = item_id
        
        # Execute query and convert to DataFrame
        cursor = price_coll.find(query)
        df = pd.DataFrame(list(cursor))
        
        # Drop MongoDB's _id column
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        logger.info(f"Retrieved {len(df)} price records from MongoDB")
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving price data from MongoDB: {e}")
        return pd.DataFrame()
    
    finally:
        client.close()

def analyze_price_trends(df):
    """
    Basic price trend analysis on the data.
    
    Args:
        df (pd.DataFrame): DataFrame with price data
    
    Returns:
        pd.DataFrame: DataFrame with trend analysis
    """
    if df.empty:
        logger.warning("No data available for analysis")
        return df
    
    # Group by item_id and item_name
    try:
        grouped = df.groupby(['item_id', 'item_name']).agg({
            'high_price_1h': ['mean', 'min', 'max', 'std'],
            'low_price_1h': ['mean', 'min', 'max', 'std'],
            'high_volume_1h': ['sum', 'mean'],
            'low_volume_1h': ['sum', 'mean'],
            'collection_time': ['min', 'max', 'count']
        })
        
        # Flatten multi-level column names
        grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
        
        # Calculate price volatility (coefficient of variation)
        grouped['high_price_volatility'] = grouped['high_price_1h_std'] / grouped['high_price_1h_mean']
        grouped['low_price_volatility'] = grouped['low_price_1h_std'] / grouped['low_price_1h_mean']
        
        # Calculate spread between high and low prices
        grouped['avg_spread'] = grouped['high_price_1h_mean'] - grouped['low_price_1h_mean']
        grouped['avg_spread_pct'] = (grouped['avg_spread'] / grouped['low_price_1h_mean']) * 100
        
        # Calculate date range
        grouped['days_covered'] = (grouped['collection_time_max'] - grouped['collection_time_min']).dt.total_seconds() / (60 * 60 * 24)
        
        return grouped.reset_index().sort_values('avg_spread_pct', ascending=False)
        
    except Exception as e:
        logger.error(f"Error analyzing price data: {e}")
        return pd.DataFrame()

def main():
    logger.info("ZAMORAK LOADED")
    
    # Get price data for the last 7 days
    df = get_price_data(days=7)
    
    if df.empty:
        logger.error("No data retrieved from MongoDB. Exiting.")
        return
    
    # Print basic stats
    print(f"Successfully loaded {len(df)} records for {df['item_id'].nunique()} unique items")
    
    # Perform trend analysis
    trends_df = analyze_price_trends(df)
    
    if not trends_df.empty:
        # Display top 10 items by spread percentage
        print("\nTop 10 Items by Price Spread %")
        print(trends_df[['item_name', 'high_price_1h_mean', 'low_price_1h_mean', 
                        'avg_spread', 'avg_spread_pct', 'high_volume_1h_sum', 
                        'low_volume_1h_sum']].head(10))
        
        # Display top 10 items by trading volume
        volume_df = trends_df.sort_values('high_volume_1h_sum', ascending=False)
        print("\nTop 10 Items by Trading Volume")
        print(volume_df[['item_name', 'high_price_1h_mean', 'high_volume_1h_sum', 
                         'low_volume_1h_sum', 'avg_spread_pct']].head(10))
        
        # Display top 10 items by price volatility
        volatile_df = trends_df.sort_values('high_price_volatility', ascending=False)
        print("\nTop 10 Items by Price Volatility")
        print(volatile_df[['item_name', 'high_price_1h_mean', 'high_price_volatility', 
                          'low_price_volatility', 'high_volume_1h_sum']].head(10))
    else:
        logger.warning("No trend analysis available")


if __name__ == "__main__":
    main()
