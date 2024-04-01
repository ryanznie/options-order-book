import os
import kalshi_python
import uuid
from pprint import pprint
from dotenv import load_dotenv, find_dotenv
from kalshi_python.models import *
from datetime import datetime, timezone, timedelta
import pandas as pd  # Make sure to import pandas


def login():

    load_dotenv(find_dotenv('.envtemplate'))

    config = kalshi_python.Configuration()
    # Comment the line below to use production
    config.host = 'https://demo-api.kalshi.co/trade-api/v2'

    # Create an API configuration passing your credentials.
    # Use this if you want the kalshi_python sdk to manage the authentication for you.
    kalshi_api = kalshi_python.ApiInstance(
        email=os.getenv('EMAIL'),
        password=os.getenv('PASSWORD'),
        configuration=config,
    )

    exchangeStatus = kalshi_api.get_exchange_status()
    pprint(exchangeStatus)
    return kalshi_api


def get_brackets(kalshi_api, days_ahead, output_format='df'):
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0) + (days_ahead * timedelta(days=1, seconds=-1))
    today_end = today_start + timedelta(days=1, seconds=-1)

    # Convert to Unix timestamp
    today_start_ts = int(today_start.timestamp())
    today_end_ts = int(today_end.timestamp())

    try:
        # Fetch markets that close today
        response = kalshi_api.get_markets(
            series_ticker="INX",
            min_close_ts=today_start_ts,
            max_close_ts=today_end_ts
        )

        markets_data = response.markets

        # Construct a list of filtered market information
        filtered_markets = [{
            'market_id': market.ticker,
            'cap_strike': market.cap_strike,
            'floor_strike': market.floor_strike,
            'last_price': market.last_price,
            'liquidity': market.liquidity,
            'no_ask': market.no_ask,
            'no_bid': market.no_bid,
            'open_interest': market.open_interest,
            'previous_price': market.previous_price,
            'previous_yes_ask': market.previous_yes_ask,
            'previous_yes_bid': market.previous_yes_bid,
            'result': market.result,
            'subtitle': market.subtitle,
            'volume': market.volume,
            'volume_24h': market.volume_24h,
            'yes_ask': market.yes_ask,
            'yes_bid': market.yes_bid
        } for market in markets_data if market is not None]

        # Depending on the output_format argument, return the appropriate format
        if output_format == 'dict':
            return {'markets': filtered_markets}
        elif output_format == 'df':
            return pd.DataFrame(filtered_markets)
        else:
            raise ValueError(
                "Invalid output_format specified. Use 'dict' or 'df'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_market_orderbook(kalshi_api, market_id, output_format='dict'):
    """
    Fetches the order book for a given market and formats the output.

    Parameters:
    - kalshi_api: The API client instance.
    - market_id: The ID of the market for which to fetch the order book.
    - output_format: 'dict' for dictionary output, 'df' for pandas DataFrame. Defaults to 'dict'.

    Returns:
    - The market order book in the specified format.
    """
    try:
        # Fetch the market order book
        response = kalshi_api.get_market_orderbook(market_id)

        # Access the order book data
        orderbook_data = response.orderbook

        # If output_format is 'dict', return the data directly
        if output_format == 'dict':
            return orderbook_data.to_dict()

        # If output_format is 'df', convert the data into a DataFrame
        elif output_format == 'df':
            # Assuming orderbook_data can be represented as a DataFrame directly
            # Adjust as necessary based on the structure of OrderBook
            return pd.DataFrame([orderbook_data.to_dict()])

        else:
            raise ValueError(
                "Invalid output_format specified. Use 'dict' or 'df'.")

    except Exception as e:
        print(f"An error occurred: {e}")


def get_combined_market_data(kalshi_api, days_ahead):
    """
    Fetches market data and the corresponding order book for each market, combining the information into a single DataFrame with only the subtitle and the orderbook.

    Parameters:
    - kalshi_api: The API client instance.

    Returns:
    - A pandas DataFrame with each market's subtitle and the corresponding order book.
    """
    # First, get the brackets (markets closing today) as a DataFrame
    brackets_df = get_brackets(kalshi_api, days_ahead, output_format='df')

    # Initialize a list to hold the subtitle and order book data for each market
    combined_data = []

    # Iterate through each market in the brackets DataFrame
    for _, market in brackets_df.iterrows():
        market_id = market['market_id']

        # Fetch the order book for the current market
        orderbook_data = get_market_orderbook(
            kalshi_api, market_id, output_format='dict')

        # Extract only the subtitle and orderbook data for the current market
        market_data = {
            'subtitle': market['subtitle'],
            'orderbook': orderbook_data
        }

        # Append the focused data to our list
        combined_data.append(market_data)

    # Convert the list to a DataFrame
    combined_df = pd.DataFrame(combined_data)

    return combined_df
