import pandas as pd
import os
import logging
import numpy as np
import json
from pymongo import MongoClient
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('zamorak')

def load_item_mapping():
    """
    Load item mapping from mapping.json file and create lookup dict for non-member items
    """
    try:
        with open('mapping.json', 'r') as f:
            items = json.load(f)
            
        # Create a lookup dict of non-member items
        non_member_items = {}
        for item in items:
            if 'members' in item and item['members'] == False:
                if 'id' in item:
                    non_member_items[str(item['id'])] = {
                        'name': item.get('name', 'Unknown'),
                        'limit': item.get('limit', 0),
                        'value': item.get('value', 0)
                    }
        
        logger.info(f"Loaded {len(non_member_items)} non-member items from mapping.json")
        return non_member_items
    except Exception as e:
        logger.error(f"Error loading item mapping: {e}")
        return {}

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

def get_price_data(hours=3, days=None, item_id=None):
    """
    Retrieve price data from MongoDB and convert to DataFrame.
    
    Args:
        hours (int, optional): Number of hours of data to retrieve
        days (int, optional): Number of days of data to retrieve
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
        if hours:
            cutoff_date = datetime.now() - timedelta(hours=hours)
            query['collection_time'] = {'$gte': cutoff_date}
        elif days:
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

def get_historical_gold_per_second(days=14, non_member_items=None):
    """
    Calculate the historical gold per second for each item over the past two weeks.
    
    Args:
        days (int): Number of days to look back for historical data
        non_member_items (dict): Dictionary of non-member items
        
    Returns:
        pd.DataFrame: DataFrame with item_id and gold_per_second
    """
    # Get two weeks of historical data
    historical_df = get_price_data(days=days)
    
    if historical_df.empty:
        logger.warning("No historical data available for gold/second calculation")
        return pd.DataFrame()
    
    try:
        # Make sure collection_time is datetime
        historical_df['collection_time'] = pd.to_datetime(historical_df['collection_time'])
        
        # Filter for non-member items only if we have the mapping
        if non_member_items:
            historical_df = historical_df[historical_df['item_id'].astype(str).isin(non_member_items.keys())]
            logger.info(f"Filtered down to {len(historical_df)} non-member item records")
        
        # Group by item_id and calculate metrics
        grouped = historical_df.groupby(['item_id', 'item_name']).apply(
            lambda x: pd.Series({
                'total_high_value': (x['high_price_1h'] * x['high_volume_1h']).sum(),
                'total_low_value': (x['low_price_1h'] * x['low_volume_1h']).sum(),
                'first_date': x['collection_time'].min(),
                'last_date': x['collection_time'].max(),
            })
        ).reset_index()
        
        # Calculate time span in seconds
        grouped['time_span_seconds'] = (grouped['last_date'] - grouped['first_date']).dt.total_seconds()
        
        # Avoid division by zero
        grouped['time_span_seconds'] = grouped['time_span_seconds'].replace(0, 1)
        
        # Calculate gold per second (average of high and low values)
        grouped['gold_per_second'] = ((grouped['total_high_value'] + grouped['total_low_value']) / 2) / grouped['time_span_seconds']
        
        return grouped[['item_id', 'item_name', 'gold_per_second']]
        
    except Exception as e:
        logger.error(f"Error calculating historical gold per second: {e}")
        return pd.DataFrame()

def analyze_items(hours=3, min_low_volume=5, non_member_items=None):
    """
    Analyze items based on ROI and volume ratio over the specified hours of data.
    
    Args:
        hours (int): Number of hours of recent data to analyze
        min_low_volume (int): Minimum low_volume_1h to consider an item viable
        non_member_items (dict): Dictionary of non-member items
        
    Returns:
        pd.DataFrame: DataFrame with analysis results
    """
    # Get recent data (last 3 hours)
    recent_df = get_price_data(hours=hours)
    
    if recent_df.empty:
        logger.warning(f"No data available for the last {hours} hours")
        return pd.DataFrame()
    
    try:
        # Make sure collection_time is datetime
        recent_df['collection_time'] = pd.to_datetime(recent_df['collection_time'])
        
        # Filter for non-member items only if we have the mapping
        if non_member_items:
            recent_df = recent_df[recent_df['item_id'].astype(str).isin(non_member_items.keys())]
            logger.info(f"Filtered down to {len(recent_df)} non-member item records")
            
            # Add item limit from mapping
            recent_df['item_limit'] = recent_df['item_id'].astype(str).map(
                lambda x: non_member_items.get(x, {}).get('limit', 0)
            )
        
        # Get the most recent data for each item (for 5-minute price spread)
        recent_prices = recent_df.sort_values('collection_time').groupby('item_id').last().reset_index()
        
        # Calculate ROI with 1% tax
        recent_prices['sell_total'] = recent_prices['high_price_1h']
        recent_prices['buy_total'] = recent_prices['low_price_1h']
        recent_prices['tax_amount'] = recent_prices['sell_total'] * 0.01  # 1% tax
        recent_prices['roi'] = (recent_prices['sell_total'] - recent_prices['tax_amount'] - recent_prices['buy_total']) / recent_prices['buy_total']
        
        # Calculate volume ratio
        recent_prices['volume_ratio'] = recent_prices['high_volume_1h'] / recent_prices['low_volume_1h'].replace(0, 1)  # Avoid division by zero
        
        # Get historical gold/second data
        gold_per_second_df = get_historical_gold_per_second(days=14, non_member_items=non_member_items)
        
        if gold_per_second_df.empty:
            logger.warning("No historical gold/second data available")
            # Continue without gold/second filter
            merged_df = recent_prices
        else:
            # Merge with historical data
            merged_df = pd.merge(recent_prices, gold_per_second_df, on=['item_id', 'item_name'], how='left')
            # Fill missing gold_per_second with 0
            merged_df['gold_per_second'] = merged_df['gold_per_second'].fillna(0)
        
        # Filter out items with insufficient low volume trading
        merged_df = merged_df[merged_df['low_volume_1h'] >= min_low_volume]
        
        # Calculate Z-scores
        merged_df['roi_zscore'] = (merged_df['roi'] - merged_df['roi'].mean()) / merged_df['roi'].std(ddof=0)
        merged_df['volume_ratio_zscore'] = (merged_df['volume_ratio'] - merged_df['volume_ratio'].mean()) / merged_df['volume_ratio'].std(ddof=0)
        
        # Replace NaN z-scores with 0 (happens when std is 0)
        merged_df['roi_zscore'] = merged_df['roi_zscore'].fillna(0)
        merged_df['volume_ratio_zscore'] = merged_df['volume_ratio_zscore'].fillna(0)
        
        # Calculate combined score
        merged_df['combined_score'] = merged_df['roi_zscore'] + merged_df['volume_ratio_zscore']
        
        # Filter out items with negative gold/second
        result_df = merged_df[merged_df['gold_per_second'] >= 0]
        
        # Filter out rows with ANY NaN values
        result_df = result_df.dropna()
        
        # Sort by combined score descending
        result_df = result_df.sort_values('combined_score', ascending=False)
        
        # Calculate expected profit per hour
        result_df['max_trades_per_hour'] = result_df['low_volume_1h']
        result_df['profit_per_trade'] = (result_df['high_price_1h'] * 0.99) - result_df['low_price_1h']
        result_df['expected_profit_per_hour'] = result_df['profit_per_trade'] * result_df['max_trades_per_hour']
        
        # Add limitation-based calculations
        if 'item_limit' in result_df.columns:
            # Calculate how many trades can be done considering GE limits
            result_df['trades_per_limit'] = result_df['item_limit'].clip(lower=1)  # Ensure no zeros
            result_df['profit_per_limit'] = result_df['profit_per_trade'] * result_df['trades_per_limit']
            
        return result_df
    
    except Exception as e:
        logger.error(f"Error analyzing items: {e}")
        return pd.DataFrame()

def main():
    logger.info("Starting Zamorak - OSRS GE Analysis")
    
    # Load non-member item mapping
    non_member_items = load_item_mapping()
    
    # Analyze items based on the last 3 hours of data with minimum low volume of 5
    # Pass non_member_items to filter out member-only items
    results = analyze_items(hours=3, min_low_volume=500, non_member_items=non_member_items)
    
    if results.empty:
        logger.error("No analysis results available. Exiting.")
        return
    
    # Print results
    print(f"Successfully analyzed {len(results)} items")
    
    # Display analysis results
    with pd.option_context(
        'display.max_columns', None,
        'display.width', None,
        'display.precision', 4,
        'display.float_format', lambda x: f"{x:.4f}" if abs(x) < 1000000 else f"{x:.0f}"
    ):
        print("\nTop 20 Items by Combined Score (ROI Z-score + Volume Ratio Z-score):")
        
        # Select and format columns for display, now including 5m prices
        display_cols = [
            'item_id', 'item_name', 'combined_score', 'roi', 
            'volume_ratio', 'low_volume_1h', 'high_volume_1h',
            'expected_profit_per_hour', 'profit_per_trade',
            'high_price_1h', 'low_price_1h',  # 1h prices
            'high_price_5m', 'low_price_5m'   # Added 5m prices
        ]
        
        print(results[display_cols].head(20))
        
        # Also show top items sorted by expected profit per hour
        profit_sorted = results.sort_values('expected_profit_per_hour', ascending=False)
        
        print("\nTop 20 Items by Expected Profit Per Hour:")
        print(profit_sorted[display_cols].head(20))
    
    return results  # Return the DataFrame for interactive use or further analysis


if __name__ == "__main__":
    main()
