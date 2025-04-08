# apps/market/management/commands/surface_signals.py
# this command finds signals for buying and stores them in firebase for retrieval to execute orders
# pip install krakenex python-dotenv celery firebase-admin schedule nltk requests

import os
import time
import uuid
import hashlib
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Tuple

import requests
import pandas as pd
import numpy as np
import krakenex

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from celery.utils.log import get_task_logger

from firebase_admin import firestore
from ...firebase_client import get_firebase_client

# Import NLTK VADER sentiment analyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Configure logging
logger = get_task_logger(__name__)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# Krakenex API Wrapper and Market Analyzer Classes
# =============================================================================

import requests
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class GDELTSentimentAnalyzer:
    def __init__(self):
        self.gdelt_base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
    def fetch_articles(self, query, mode="artlist", format="json", max_records=100):
        """
        Fetch news articles from GDELT Project
        
        Parameters:
        query (str): Search query string
        mode (str): GDELT search mode (artlist, timelinevolinfo, etc)
        format (str): Response format (json, html, etc)
        max_records (int): Maximum number of records to return
        
        Returns:
        list: List of article dictionaries
        """
        params = {
            'query': query,
            'mode': mode,
            'format': format,
            'maxrecords': max_records,
            'sort': 'DateDesc'  # Most recent first
        }
        
        try:
            response = requests.get(self.gdelt_base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # GDELT returns articles in 'articles' key
            if 'articles' in data:
                return data['articles']
            return []
            
        except Exception as e:
            print(f"Error fetching articles from GDELT: {e}")
            return []
    
    def build_query_string(self, asset):
        """Build appropriate query string for crypto assets."""
        return f'"{asset}" crypto OR cryptocurrency OR blockchain'
    
    def analyze_articles(self, articles, asset):
        """
        Analyze sentiment of articles related to an asset
        
        Parameters:
        articles (list): List of article dictionaries
        asset (str): Asset name/symbol to analyze
        
        Returns:
        tuple: (avg_sentiment, event_count)
        """
        total_sentiment = 0
        count = 0
        total_events = 0
        
        for article in articles:
            # Extract content from article
            content = ' '.join(filter(None, [
                article.get('title', ''),
                article.get('seendate', ''),
                article.get('socialimage', ''),
                article.get('domain', '')
            ]))
            
            if not content:
                continue
                
            # Analyze sentiment using VADER
            scores = self.sentiment_analyzer.polarity_scores(content)
            sentiment = scores['compound']
            
            total_sentiment += sentiment
            count += 1
            total_events += content.lower().count(asset.lower())
            
        if count == 0:
            return 0, 0
            
        avg_sentiment = total_sentiment / count
        return avg_sentiment, total_events
    
    def compute_sentiment_for_asset(self, asset):
        """
        Compute sentiment score for a specific asset
        
        Parameters:
        asset (str): Asset name/symbol
        
        Returns:
        tuple: (avg_sentiment, event_count)
        """
        query = self.build_query_string(asset)
        articles = self.fetch_articles(query)
        return self.analyze_articles(articles, asset)


class KrakenexAPI:
    def __init__(self, api_key: str, api_secret: str):
        self.api = krakenex.API(key=api_key, secret=api_secret)
        self.db = None

    def initialize_db(self):
        try:
            if self.db is None:
                logger.info("Attempting to initialize database connection...")
                self.db = get_firebase_client()
                if self.db:
                    logger.info("✓ Database connection successfully initialized")
                else:
                    logger.error("Database connection returned None")
                    raise ValueError("Database initialization failed")
            return self.db
        except Exception as e:
            logger.error(f"❌ Error initializing database: {str(e)}", exc_info=True)
            raise

    def generate_doc_id(self, symbol: str, timestamp: int) -> str:
        unique_string = f"{symbol}_{timestamp}"
        return hashlib.md5(unique_string.encode()).hexdigest()

    def get_balance(self) -> dict:
        result = self.api.query_private("Balance")
        if result.get('error'):
            logger.error(f"Error fetching balance: {result['error']}")
            return {}
        balances = result.get('result', {})
        formatted_balances = {
            asset: '{:.8f}'.format(float(amount)).rstrip('0').rstrip('.')
            for asset, amount in balances.items() if float(amount) > 0
        }
        return formatted_balances

    def get_ticker(self, pair: str) -> dict:
        data = self.api.query_public("Ticker", {"pair": pair})
        if data.get('error'):
            logger.error(f"Error fetching ticker {pair}: {data['error']}")
            return {}
        result = data.get('result', {})
        ticker_data = result.get(pair)
        if ticker_data:
            try:
                current_price = float(ticker_data['c'][0])
                return {'price': current_price}
            except (IndexError, ValueError) as e:
                logger.error(f"Error parsing price data: {e}")
        return {}

    def query_orders(self, txid: str) -> dict:
        data = self.api.query_private("QueryOrders", {"txid": txid})
        if data.get('error'):
            logger.error(f"Error fetching order info: {data['error']}")
            return {}
        return data.get('result', {})

    def get_asset_info(self, pair: str) -> dict:
        data = self.api.query_public("AssetPairs", {"pair": pair})
        if data.get('error'):
            logger.error(f"Error fetching asset info: {data['error']}")
            return {}
        pair_info = data.get('result', {}).get(pair)
        logger.info(f"Pair info for {pair}: {pair_info}")
        if pair_info:
            return {
                'ordermin': float(pair_info.get('ordermin', 0)),
                'lot_decimals': int(pair_info.get('lot_decimals', 0)),
                'pair_decimals': int(pair_info.get('pair_decimals', 0)),
                'cost_decimals': int(pair_info.get('cost_decimals', 0)),
                'base': pair_info.get('base'),
                'quote': pair_info.get('quote')
            }
        return {}

class KrakenMarketAnalyzer:
    def __init__(self, api_key: str, api_secret: str):
        self.base_url = "https://api.kraken.com/0/public"
        self.session = requests.Session()
        self.logger = logger
        self.last_request_time = 0
        self.MIN_REQUEST_INTERVAL = 1.5
        self.api_key = api_key
        self.api_secret = api_secret
        self.kraken_api = KrakenexAPI(api_key, api_secret)

    def _rate_limited_request(self, endpoint: str, params: Dict = None) -> Dict:
        current_time_ = time.time()
        time_since_last = current_time_ - self.last_request_time
        if time_since_last < self.MIN_REQUEST_INTERVAL:
            time.sleep(self.MIN_REQUEST_INTERVAL - time_since_last)
        try:
            url = f"{self.base_url}/{endpoint}"
            response = self.session.get(url, params=params)
            self.last_request_time = time.time()
            response.raise_for_status()
            data = response.json()
            if data.get('error'):
                self.logger.error(f"Kraken API error: {data['error']}")
                return {}
            return data.get('result', {})
        except Exception as e:
            self.logger.error(f"Error making request to {endpoint}: {e}")
            return {}

    def get_balance(self) -> dict:
        return self.kraken_api.get_balance()

    def get_top_volume_volatile_pairs(self, min_volume_usd: float = 1000000) -> List[str]:
        """
        Get top ~20 or 50 volume-volatile pairs (only pairs ending in USD/ZUSD).
        Similar to the approach in kraken_profit.py
        """
        # Get asset pairs
        pairs_data = self._rate_limited_request("AssetPairs")
        # Filter for crypto pairs ending with ZUSD (like in kraken_profit.py)
        pairs = [p for p in pairs_data.keys() if pairs_data[p]['quote'] == 'ZUSD']

        if not pairs:
            self.logger.error("No pairs found.")
            return []

        # Get 24h ticker data
        ticker_data = self._rate_limited_request("Ticker", {"pair": ','.join(pairs)})
        if not ticker_data:
            return []

        # Process ticker data
        pair_metrics = []
        for pair, info in ticker_data.items():
            try:
                volume_24h = float(info['v'][1])  # 24h volume
                vwap = float(info['p'][1])       # 24h VWAP
                high = float(info['h'][1])       # 24h high
                low = float(info['l'][1])        # 24h low

                volume_usd = volume_24h * vwap
                if vwap > 0:
                    volatility = (high - low) / vwap
                else:
                    volatility = 0

                if volume_usd >= min_volume_usd:
                    pair_metrics.append({
                        'pair': pair,
                        'volume_usd': volume_usd,
                        'volatility': volatility
                    })

            except (KeyError, ValueError, ZeroDivisionError) as e:
                self.logger.warning(f"Error processing {pair}: {e}")
                continue

        # Sort and get top ~10
        df = pd.DataFrame(pair_metrics)
        if df.empty:
            return []

        df['volume_rank'] = df['volume_usd'].rank(ascending=False)
        df['volatility_rank'] = df['volatility'].rank(ascending=False)
        df['combined_rank'] = df['volume_rank'] + df['volatility_rank']

        top_pairs = df.nsmallest(10, 'combined_rank')['pair'].tolist()
        
        self.logger.info(f"Top crypto pairs for analysis: {top_pairs}")
        return top_pairs
    
        
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators based on the research paper findings.
        Key strategies to implement:
        1. Triple EMA (highest average percentage gain)
        2. EMA + Heikin Ashi + Parabolic SAR (highest win rate)
        3. TTM Squeeze + EMA (highest number of winning trades)
        """
        df = df.copy()
        
        # Original indicators
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        
        # Stochastic
        low_min = df['low'].rolling(14).min()
        high_max = df['high'].rolling(14).max()
        df['stoch_k'] = 100 * ((df['close'] - low_min) / (high_max - low_min))
        df['stoch_d'] = df['stoch_k'].rolling(3).mean()
        
        # Add new indicators from the research paper
        
        # 1. Triple EMA (9, 13, 21)
        df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_13'] = df['close'].ewm(span=13, adjust=False).mean()
        df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
        
        # 2. EMA + Heikin Ashi + Parabolic SAR
        # EMA (200) for trend direction
        df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
        
        # Heikin Ashi calculations
        df['ha_open'] = (df['open'].shift(1) + df['close'].shift(1)) / 2
        df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
        df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
        
        # Determine Heikin Ashi candle color (green/red)
        df['ha_color'] = np.where(df['ha_close'] >= df['ha_open'], 1, -1)  # 1 for green, -1 for red
        
        # Parabolic SAR (using simple approximation - for actual implementation consider using TA-Lib)
        # This is a simplified version - actual PSAR is more complex
        af = 0.02  # acceleration factor
        max_af = 0.2
        df['psar'] = df['close'].copy()
        df['psar_direction'] = 0
        
        for i in range(2, len(df)):
            if df['close'].iloc[i-1] > df['psar'].iloc[i-1]:
                # Uptrend
                df.loc[df.index[i], 'psar_direction'] = 1
                df.loc[df.index[i], 'psar'] = max(df['low'].iloc[i-2:i].min(), df['psar'].iloc[i-1])
            else:
                # Downtrend
                df.loc[df.index[i], 'psar_direction'] = -1
                df.loc[df.index[i], 'psar'] = min(df['high'].iloc[i-2:i].max(), df['psar'].iloc[i-1])
        
        # 3. TTM Squeeze
        # Bollinger Bands
        df['sma_20'] = df['close'].rolling(window=20).mean()
        df['stddev'] = df['close'].rolling(window=20).std()
        df['bolu'] = df['sma_20'] + 2 * df['stddev']  # Upper Bollinger Band
        df['bold'] = df['sma_20'] - 2 * df['stddev']  # Lower Bollinger Band
        
        # Keltner Channels
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        atr_high = df['high'] - df['low']
        atr_high_low = abs(df['high'] - df['close'].shift(1))
        atr_low_high = abs(df['low'] - df['close'].shift(1))
        df['tr'] = pd.concat([atr_high, atr_high_low, atr_low_high], axis=1).max(axis=1)
        df['atr_20'] = df['tr'].rolling(window=20).mean()
        
        df['kcu'] = df['ema_20'] + 2 * df['atr_20']  # Upper Keltner Channel
        df['kcd'] = df['ema_20'] - 2 * df['atr_20']  # Lower Keltner Channel
        
        # TTM Squeeze test: BB inside KC means squeeze is on
        df['squeeze_on'] = (df['bold'] > df['kcd']) & (df['bolu'] < df['kcu'])
        
        # Momentum oscillator for TTM
        df['ttm_momentum'] = df['close'] - ((df['high'].rolling(window=20).max() + df['low'].rolling(window=20).min())/2 + df['sma_20'])/2
        df['ttm_signal'] = df['ttm_momentum'].ewm(span=1).mean()
        
        # 4. SSL Channel
        df['ssl_sma_high'] = df['high'].rolling(window=10).mean()
        df['ssl_sma_low'] = df['low'].rolling(window=10).mean()
        df['ssl_up'] = np.where(df['close'] > df['ssl_sma_high'], df['ssl_sma_high'], df['ssl_sma_low'])
        df['ssl_down'] = np.where(df['close'] > df['ssl_sma_high'], df['ssl_sma_low'], df['ssl_sma_high'])
        
        # 5. STC (Schaff Trend Cycle)
        # MACD line calculation
        df['stc_macd'] = df['ema_9'] - df['ema_21']
        # %K of MACD
        df['stc_macd_k'] = 100 * (df['stc_macd'] - df['stc_macd'].rolling(window=10).min()) / (df['stc_macd'].rolling(window=10).max() - df['stc_macd'].rolling(window=10).min())
        # %D of %K of MACD
        df['stc_macd_d'] = df['stc_macd_k'].rolling(window=3).mean()
        # STC
        df['stc'] = 100 * (df['stc_macd_d'] - df['stc_macd_d'].rolling(window=10).min()) / (df['stc_macd_d'].rolling(window=10).max() - df['stc_macd_d'].rolling(window=10).min())
        
        # 6. Vortex Indicator
        period = 25  # The paper mentions using 25 instead of 14 to reduce false signals
        df['tr'] = df['tr']  # Already calculated for Keltner Channels
        
        vm_plus = abs(df['high'] - df['low'].shift(1))
        vm_minus = abs(df['low'] - df['high'].shift(1))
        
        df['vm_plus'] = vm_plus.rolling(window=period).sum()
        df['vm_minus'] = vm_minus.rolling(window=period).sum()
        df['tr_sum'] = df['tr'].rolling(window=period).sum()
        
        df['vortex_plus'] = df['vm_plus'] / df['tr_sum']
        df['vortex_minus'] = df['vm_minus'] / df['tr_sum']
        
        # 7. Weighted Moving Average (WMA) for stochastic strategy
        weights = np.arange(1, 10) / np.arange(1, 10).sum()
        df['wma_9'] = df['close'].rolling(window=9).apply(lambda x: np.sum(weights * x[-9:]))
        
        return df

    def analyze_signals(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Analyze the dataframe for trading signals based on the best strategies 
        identified in the paper.
        """
        current = df.iloc[-1]
        previous = df.iloc[-2]
        
        # Basic signals (from original code)
        signals = {
            'rsi_oversold': bool(current['rsi'] < 30),
            'macd_crossover': bool(current['macd'] > current['macd_signal'] and 
                                previous['macd'] <= previous['macd_signal']),
            'ema_crossover': bool(current['ema_9'] > current['ema_20'] and 
                                previous['ema_9'] <= previous['ema_20']),
            'stoch_oversold_cross': bool(current['stoch_k'] > current['stoch_d'] and 
                                    previous['stoch_k'] <= previous['stoch_d'] and 
                                    current['stoch_k'] < 30)
        }
        
        # Add signals from the paper's top-performing strategies
        
        # 1. Triple EMA Strategy
        signals['triple_ema_buy'] = bool(
            current['ema_9'] > current['ema_13'] > current['ema_21'] and
            not (previous['ema_9'] > previous['ema_13'] > previous['ema_21'])
        )
        
        # 2. EMA + Heikin Ashi + Parabolic SAR Strategy
        signals['ema_ha_psar_buy'] = bool(
            current['close'] > current['ema_200'] and  # Above EMA 200
            current['ha_color'] == 1 and  # Green Heikin Ashi candle
            current['psar_direction'] == 1 and  # PSAR below candle (uptrend)
            previous['psar_direction'] == -1  # Previous PSAR was above candle (crossover)
        )
        
        # 3. TTM Squeeze + EMA Strategy
        signals['ttm_squeeze_buy'] = bool(
            current['close'] > current['ema_200'] and  # Above EMA 200
            current['squeeze_on'] == False and  # Squeeze is firing (was on, now off)
            previous['squeeze_on'] == True and
            current['ttm_momentum'] > 0 and  # Positive momentum
            current['ttm_momentum'] > current['ttm_signal']  # Increasing momentum
        )
        
        # 4. SSL Channel + EMA Strategy
        signals['ssl_channel_buy'] = bool(
            current['close'] > current['ema_200'] and  # Above EMA 200
            current['ssl_up'] > current['ssl_down'] and  # Positive SSL (green above red)
            previous['ssl_up'] <= previous['ssl_down']  # Crossover just happened
        )
        
        # 5. STC + EMA Strategy
        signals['stc_buy'] = bool(
            current['close'] > current['ema_200'] and  # Above EMA 200
            current['stc'] > 25 and  # STC crosses above 25
            previous['stc'] <= 25
        )
        
        # 6. Vortex + EMA Strategy
        signals['vortex_buy'] = bool(
            current['close'] > current['ema_200'] and  # Above EMA 200
            current['vortex_plus'] > current['vortex_minus'] and  # VI+ crosses above VI-
            previous['vortex_plus'] <= previous['vortex_minus']
        )
        
        # 7. Stochastic + WMA Strategy
        signals['stoch_wma_buy'] = bool(
            current['close'] > current['ema_200'] and  # Above EMA 200
            current['stoch_k'] < 5 and  # RSI is extremely oversold
            current['close'] > current['wma_9']  # Price above WMA
        )
        
        # Weight the strategies based on the paper's findings
        strategy_weights = {
            'triple_ema_buy': 0.2,          # High percentage gain but lower win rate
            'ema_ha_psar_buy': 0.25,        # Best overall win rate (>50%)
            'ttm_squeeze_buy': 0.2,         # Highest number of winning trades
            'ssl_channel_buy': 0.1,
            'stc_buy': 0.1,
            'vortex_buy': 0.1,
            'stoch_wma_buy': 0.05,
            # Original signals with reduced weight
            'rsi_oversold': 0.03,
            'macd_crossover': 0.03,
            'ema_crossover': 0.02,
            'stoch_oversold_cross': 0.02
        }
        
        # Calculate weighted buy score
        buy_score = sum(value * strategy_weights[key] for key, value in signals.items() if key in strategy_weights)
        
        # Normalize to [0,1]
        signals['buy_score'] = float(buy_score)
        
        return signals

    def determine_trend(self, df: pd.DataFrame) -> str:
        """
        Enhanced trend determination using multiple strategies from the research paper.
        """
        current = df.iloc[-1]
        
        # Trend signals
        trend_strength = 0
        
        # EMA relationships (Triple EMA)
        if current['ema_9'] > current['ema_13'] > current['ema_21']:
            trend_strength += 2  # Strong uptrend signal
        elif current['ema_9'] < current['ema_13'] < current['ema_21']:
            trend_strength -= 2  # Strong downtrend signal
        
        # Heikin Ashi color
        if current['ha_color'] == 1:  # Green
            trend_strength += 1
        else:  # Red
            trend_strength -= 1
        
        # Parabolic SAR direction
        if current['psar_direction'] == 1:  # Below price (bullish)
            trend_strength += 1
        else:  # Above price (bearish)
            trend_strength -= 1
        
        # RSI position
        if current['rsi'] > 60:
            trend_strength += 1
        elif current['rsi'] < 40:
            trend_strength -= 1
        
        # MACD position
        if current['macd'] > current['macd_signal']:
            trend_strength += 1
        else:
            trend_strength -= 1
            
        # TTM Squeeze momentum
        if current['ttm_momentum'] > 0:
            trend_strength += 1
        else:
            trend_strength -= 1
        
        # Categorize trend based on strength
        if trend_strength >= 4:
            return "Strong Uptrend"
        elif trend_strength >= 1:
            return "Moderate Uptrend"
        elif trend_strength <= -4:
            return "Strong Downtrend"
        elif trend_strength <= -1:
            return "Moderate Downtrend"
        else:
            return "Sideways/Neutral"
        
        
    def get_pair_analysis(self, pair: str) -> Dict:
        ohlcv_data = self._rate_limited_request("OHLC", {"pair": pair, "interval": 240})
        if not ohlcv_data or pair not in ohlcv_data:
            self.logger.error(f"No OHLC data found for pair: {pair}")
            return {}
        df = pd.DataFrame(ohlcv_data[pair],
                          columns=['timestamp','open','high','low','close','vwap','volume','count'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        numeric_columns = ['open', 'high', 'low', 'close', 'vwap', 'volume']
        for col in numeric_columns:
            df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        initial_row_count = len(df)
        df.dropna(subset=numeric_columns, inplace=True)
        if len(df) < initial_row_count:
            self.logger.warning(f"Dropped {initial_row_count - len(df)} rows due to conversion errors.")
        df.set_index('timestamp', inplace=True)
        analysis = self.calculate_indicators(df)
        signals = self.analyze_signals(analysis)
        trend = self.determine_trend(analysis)
        try:
            current_close = df['close'].iloc[-1]
            current_price = round(float(current_close), 8)
        except Exception as e:
            self.logger.error(f"Error extracting current price for {pair}: {e}")
            return {}
        try:
            indicators = {
                'rsi': float(analysis['rsi'].iloc[-1]),
                'macd': float(analysis['macd'].iloc[-1]),
                'macd_signal': float(analysis['macd_signal'].iloc[-1]),
                'ema_9': float(analysis['ema_9'].iloc[-1]),
                'ema_20': float(analysis['ema_20'].iloc[-1]),
                'stoch_k': float(analysis['stoch_k'].iloc[-1]),
                'stoch_d': float(analysis['stoch_d'].iloc[-1])
            }
        except Exception as e:
            self.logger.error(f"Error extracting indicators for {pair}: {e}")
            return {}
            
        # Get additional pair info
        asset_info = self.kraken_api.get_asset_info(pair)
        
        return {
            'pair': pair,
            'current_price': current_price,
            'trend': trend,
            'signals': signals,
            'buy_score': signals.get('buy_score', 0),
            'indicators': indicators,
            'timestamp': datetime.utcnow().isoformat()
        }

    def get_complete_market_analysis(self) -> List[Dict]:
        self.logger.info("Starting market analysis...")
        
        # Get only crypto pairs that end with ZUSD (similar to kraken_profit.py)
        crypto_pairs = self.get_top_volume_volatile_pairs(min_volume_usd=1_000_000)
        
        if not crypto_pairs:
            self.logger.error("No crypto pairs found with sufficient volume.")
            return []
            
        signals_list = []
        for pair in crypto_pairs:
            analysis = self.get_pair_analysis(pair)
            if analysis:
                signals_list.append(analysis)
                
        return signals_list


# =============================================================================
# News Sentiment Analyzer Class
# =============================================================================
class NewsSentimentAnalyzer:
    def __init__(self):
        self.NEWS_API_KEY = os.getenv("NEWS_API_KEY")
        self.NEWS_API_URL = 'https://newsapi.org/v2/everything'
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

    def build_query_string(self, asset: str) -> str:
        """Build appropriate query string for crypto assets."""
        return f'("{asset}") AND (crypto OR cryptocurrency OR blockchain)'

    def fetch_articles(self, query: str) -> List[Dict]:
        params = {
            'q': query,
            'language': 'en',
            'sortBy': 'publishedAt',
            'apiKey': self.NEWS_API_KEY,
            'pageSize': 20,
        }
        response = requests.get(self.NEWS_API_URL, params=params)
        data = response.json()
        if data.get('status') != 'ok':
            logger.error(f"News API error: {data.get('message')}")
            return []
        return data.get('articles', [])

    def analyze_articles(self, articles: List[Dict], asset: str) -> Tuple[float, int]:
        total_sentiment = 0
        count = 0
        total_events = 0
        for article in articles:
            content = ' '.join(filter(None, [
                article.get('title', ''),
                article.get('description', ''),
                article.get('content', '')
            ]))
            if not content:
                continue
            scores = self.sentiment_analyzer.polarity_scores(content)
            sentiment = scores['compound']
            total_sentiment += sentiment
            count += 1
            total_events += content.lower().count(asset.lower())
        if count == 0:
            return 0, 0
        avg_sentiment = total_sentiment / count
        return avg_sentiment, total_events

    def compute_sentiment_for_asset(self, asset: str) -> Tuple[float, int]:
        query = self.build_query_string(asset)
        articles = self.fetch_articles(query)
        return self.analyze_articles(articles, asset)

# =============================================================================
# Combined Command: Surface Best Signals with News Sentiment
# =============================================================================
class Command(BaseCommand):
    help = 'Surfaces the best market signals for crypto pairs by combining Kraken market signals with GDELT news sentiment analysis.'

    def handle(self, *args, **options):
        # Initialize Firebase and analyzers
        db = get_firebase_client()
        if not db:
            raise CommandError("Failed to initialize Firebase connection.")

        api_key = settings.KRAKEN_API_KEY
        api_secret = settings.KRAKEN_API_SECRET
        market_analyzer = KrakenMarketAnalyzer(api_key, api_secret)
        
        # Use GDELT for sentiment analysis
        news_analyzer = GDELTSentimentAnalyzer()

        # Get market signals (list of dicts) - now only for crypto pairs
        signals = market_analyzer.get_complete_market_analysis()
        if not signals:
            self.stdout.write(self.style.WARNING("No market signals produced in this cycle."))
            return

        combined_signals = []
        # For each signal, get the base asset and fetch news sentiment
        for signal in signals:
            pair = signal.get('pair')
            if not pair:
                continue
                
            # Get asset info to extract the base (asset symbol)
            asset_info = market_analyzer.kraken_api.get_asset_info(pair)
            
            # For crypto, use the base asset for sentiment analysis
            asset = asset_info.get('base', '')
            if not asset:
                self.stdout.write(self.style.WARNING(f"Could not determine asset for pair {pair}. Skipping."))
                continue

            # Use GDELT for sentiment analysis
            avg_sentiment, event_count = news_analyzer.compute_sentiment_for_asset(asset)
            
            # Normalize news sentiment to [0,1]
            normalized_sentiment = (avg_sentiment + 1) / 2  # because avg_sentiment is in [-1,1]
            
            # Compute a composite score (example: 60% weight for market buy_score, 40% for news sentiment)
            market_score = signal.get('buy_score', 0)
            
            # Use adaptive weighting: more events = higher trust in sentiment
            sentiment_weight = min(0.4, 0.1 + (event_count / 100) * 0.3)  # Scale from 0.1 to 0.4 based on events
            market_weight = 1 - sentiment_weight
            
            composite_score = market_weight * market_score + sentiment_weight * normalized_sentiment

            # Add news fields and composite score to the signal dictionary
            signal['asset'] = asset
            signal['news_avg_sentiment'] = avg_sentiment
            signal['news_event_count'] = event_count
            signal['sentiment_weight'] = sentiment_weight
            signal['market_weight'] = market_weight
            signal['composite_score'] = composite_score
            signal['sentiment_source'] = 'GDELT'  # Adding source info
            combined_signals.append(signal)
            
            logger.info(f"Signal for {pair} ({asset}): Market score {market_score:.2f}, "
                        f"News sentiment {avg_sentiment:.2f} (normalized {normalized_sentiment:.2f}), "
                        f"Events: {event_count}, Composite {composite_score:.2f}")

        # Sort signals by composite score descending
        combined_signals.sort(key=lambda s: s.get('composite_score', 0), reverse=True)
        top_signals = combined_signals[:5]
        self.stdout.write(self.style.SUCCESS(f"Selected top {len(top_signals)} signals based on composite score."))

        # Store top signals in Firebase collection "signals"
        for signal in top_signals:
            doc_id = market_analyzer.kraken_api.generate_doc_id(signal['pair'], int(time.time()))
            db.collection('signals').document(doc_id).set(signal)
            logger.info(f"Stored best signal for {signal['pair']} with doc_id {doc_id}")

        self.stdout.write(self.style.SUCCESS(f"Successfully stored {len(top_signals)} best signals in Firebase."))