"""
Data package for Jamcakes v26 Forecaster.

Includes:
- fetch_data: unified Yahoo + Polygon data retrieval
- caching and normalization
"""
from .fetch_data import fetch_ohlcv, latest_close_date
