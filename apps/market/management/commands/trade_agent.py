# apps/market/management/commands/trade_agent.py
# this command retrieves signals from firebase, and places buy orders
# pip install krakenex python-dotenv celery firebase-admin schedule


# The code retrieves trade signals stored in Firebase, ranks them by prioritizing forex signals with strong technical indicators, and executes corresponding cryptocurrency buy orders on the Kraken exchange (including placing associated take-profit and stop-loss orders). It also periodically calculates and logs the user's portfolio valuation in USD based on current market data.
import krakenex
import requests
import pandas as pd
import numpy as np
import time
import logging
import threading
import uuid
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_DOWN
# import schedule
from django.core.management.base import BaseCommand
from django.conf import settings
from celery import shared_task
from firebase_admin import firestore
import os
from ...firebase_client import get_firebase_client
from dotenv import load_dotenv
from datetime import datetime, timedelta
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


load_dotenv()

# ----------------------------------------------------------------------------
#                         Krakenex API Wrapper
# ----------------------------------------------------------------------------
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
        import hashlib
        return hashlib.md5(unique_string.encode()).hexdigest()

    def get_trades(self, trade_id: str) -> List[str]:
        try:
            if not self.db:
                logger.error("Database connection not initialized")
                return []
            docs = (self.db.collection('trades')
                    .where('trade_id', '==', trade_id)
                    .get())
            return docs
        except Exception as e:
            logger.error(e)
            return []

    def get_balance(self) -> dict:
        result = self.api.query_private("Balance")
        if result.get('error'):
            logger.error(f"Error fetching balance: {result['error']}")
            return {}
        balances = result.get('result', {})
        formatted_balances = {
            asset: '{:.8f}'.format(float(amount)).rstrip('0').rstrip('.')
            for asset, amount in balances.items()
            if float(amount) > 0
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

# ----------------------------------------------------------------------------
#               Market Analyzer for Crypto & Forex
# ----------------------------------------------------------------------------
class KrakenMarketAnalyzer:
    def __init__(self, api_key: str, api_secret: str):
        self.base_url = "https://api.kraken.com/0/public"
        self.session = requests.Session()
        self.logger = logging.getLogger('KrakenAnalyzer')
        self.last_request_time = 0
        self.MIN_REQUEST_INTERVAL = 1.5
        self.api_key = api_key
        self.api_secret = api_secret
        # Use our newly created KrakenexAPI
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
        """Wrapper around the private get_balance call."""
        return self.kraken_api.get_balance()

    def get_top_volume_volatile_pairs(self, min_volume_usd: float = 1_000_000) -> List[str]:
        pairs_data = self._rate_limited_request("AssetPairs")
        pairs = [p for p in pairs_data.keys() if pairs_data[p]['quote'] == 'ZUSD']
        if not pairs:
            self.logger.error("No pairs found.")
            return []
        ticker_data = self._rate_limited_request("Ticker", {"pair": ','.join(pairs)})
        if not ticker_data:
            return []
        pair_metrics = []
        for pair, info in ticker_data.items():
            try:
                volume_24h = float(info['v'][1])
                vwap = float(info['p'][1])
                high = float(info['h'][1])
                low = float(info['l'][1])
                volume_usd = volume_24h * vwap
                volatility = (high - low) / vwap if vwap > 0 else 0
                if volume_usd >= min_volume_usd:
                    pair_metrics.append({
                        'pair': pair,
                        'volume_usd': volume_usd,
                        'volatility': volatility
                    })
            except (KeyError, ValueError, ZeroDivisionError) as e:
                self.logger.warning(f"Error processing {pair}: {e}")
                continue
        df = pd.DataFrame(pair_metrics)
        if df.empty:
            return []
        df['volume_rank'] = df['volume_usd'].rank(ascending=False)
        df['volatility_rank'] = df['volatility'].rank(ascending=False)
        df['combined_rank'] = df['volume_rank'] + df['volatility_rank']
        top_20 = df.nsmallest(10, 'combined_rank')['pair'].tolist()
        print(top_20)
        return top_20

    # --------------------------
    # Technical Indicators
    # --------------------------
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
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
        # EMAs
        df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        # Stochastic
        low_min = df['low'].rolling(14).min()
        high_max = df['high'].rolling(14).max()
        df['stoch_k'] = 100 * ((df['close'] - low_min) / (high_max - low_min))
        df['stoch_d'] = df['stoch_k'].rolling(3).mean()
        return df

    def analyze_signals(self, df: pd.DataFrame) -> Dict[str, bool]:
        current = df.iloc[-1]
        previous = df.iloc[-2]
        signals = {
            'rsi_oversold': (current['rsi'] < 30),
            'macd_crossover': (current['macd'] > current['macd_signal'] and previous['macd'] <= previous['macd_signal']),
            'ema_crossover': (current['ema_9'] > current['ema_20'] and previous['ema_9'] <= previous['ema_20']),
            'stoch_oversold_cross': (current['stoch_k'] > current['stoch_d'] and previous['stoch_k'] <= previous['stoch_d'] and current['stoch_k'] < 30)
        }
        signals['buy_score'] = sum(1 for s in signals.values() if s) / len(signals)
        return signals

    def determine_trend(self, df: pd.DataFrame) -> str:
        current = df.iloc[-1]
        if current['ema_9'] > current['ema_20']:
            return "Strong Uptrend" if current['rsi'] > 50 else "Moderate Uptrend"
        else:
            return "Strong Downtrend" if current['rsi'] < 50 else "Moderate Downtrend"

    def get_pair_analysis(self, pair: str) -> Dict:
        ohlcv_data = self._rate_limited_request("OHLC", {"pair": pair, "interval": 240})
        if not ohlcv_data or pair not in ohlcv_data:
            self.logger.error(f"No OHLC data found for pair: {pair}")
            return {}
        df = pd.DataFrame(
            ohlcv_data[pair],
            columns=['timestamp','open','high','low','close','vwap','volume','count']
        )
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
        return {
            'pair': pair,
            'current_price': current_price,
            'trend': trend,
            'signals': signals,
            'buy_score': signals.get('buy_score', 0),
            'indicators': indicators
        }

    def get_complete_market_analysis(self) -> pd.DataFrame:
        self.logger.info("Starting market analysis...")
        top_pairs = self.get_top_volume_volatile_pairs(min_volume_usd=1_000_000)
        self.logger.info(f"Found {len(top_pairs)} pairs for analysis")
        analyses = []
        for pair in top_pairs:
            analysis = self.get_pair_analysis(pair)
            if analysis:
                sig = analysis['signals']
                analyses.append({
                    'symbol': pair,
                    'price': analysis['current_price'],
                    'trend': analysis['trend'],
                    'buy_score': sig.get('buy_score', 0),
                    'rsi': analysis['indicators']['rsi'],
                    'macd': analysis['indicators']['macd'],
                    'active_signals': sum(1 for s in sig.values() if s and s != 'buy_score'),
                    'signal_details': ', '.join(name for name, val in sig.items() if val and name != 'buy_score')
                })
        if not analyses:
            return pd.DataFrame()
        df = pd.DataFrame(analyses)
        df.sort_values('buy_score', ascending=False, inplace=True)
        df['price'] = df['price'].apply(lambda x: f"${x:,.8f}")
        df['buy_score'] = df['buy_score'].apply(lambda x: f"{x * 100:.1f}%")
        df['rsi'] = df['rsi'].apply(lambda x: f"{x:.1f}")
        return df

# ----------------------------------------------------------------------------
#               Trading Logic using krakenex (Order Execution)
# ----------------------------------------------------------------------------
class KrakenTradingStrategy:
    """
    This class integrates Firebase to store trade records. It places orders (limit BUY with associated TP/SL)
    and updates the trade record in Firebase.
    """
    def __init__(self, api_key, api_secret):
        self.api = krakenex.API(key=api_key, secret=api_secret)
        self.logger = self._setup_logger()
        self.firebase_db = get_firebase_client()

    def _setup_logger(self):
        logger = logging.getLogger('KrakenTrading')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not logger.handlers:
            logger.addHandler(handler)
        return logger

    def _format_decimal(self, value: float, decimal_places: int = 4) -> str:
        quantize_str = '1.' + '0' * decimal_places
        return format(Decimal(value).quantize(Decimal(quantize_str), rounding=ROUND_DOWN), 'f')

    def store_trade(self, trade_data: dict) -> str:
        trade_id = str(uuid.uuid4())
        trade_data['created_at'] = datetime.utcnow()
        trade_data['updated_at'] = datetime.utcnow()
        self.firebase_db.collection('trades').document(trade_id).set(trade_data)
        self.logger.info(f"Trade stored in Firebase with ID: {trade_id}")
        return trade_id

    def _get_pair_details(self, pair: str) -> dict:
        resp = self.api.query_public("AssetPairs", {"pair": pair})
        if resp.get("error"):
            self.logger.error(f"Error fetching asset info for pair={pair}: {resp['error']}")
            return {}
        return resp.get("result", {}).get(pair, {})

    def _get_balance(self) -> dict:
        resp = self.api.query_private("Balance")
        if resp.get("error"):
            self.logger.error(f"Error fetching balance: {resp['error']}")
            return {}
        return resp.get("result", {})

    def check_order_status(self, order_id):
        try:
            order_info = self.api.query_private("QueryOrders", {"txid": order_id})
            if order_info.get('error'):
                self.logger.error(f"Error querying order: {order_info['error']}")
                return None
            result = order_info.get('result', {})
            if order_id in result:
                return result[order_id].get("status")
            return None
        except Exception as e:
            self.logger.error(f"Error checking order status: {str(e)}")
            return None

    def cancel_order(self, order_id):
        try:
            cancel_info = self.api.query_private("CancelOrder", {"txid": order_id})
            if cancel_info.get('error'):
                self.logger.error(f"Error cancelling order: {cancel_info['error']}")
                return False
            self.logger.info(f"Successfully cancelled order {order_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error cancelling order: {str(e)}")
            return False

    def update_trade(self, trade_id: str, updates: dict):
        updates['updated_at'] = datetime.utcnow()
        self.firebase_db.collection('trades').document(trade_id).update(updates)
        self.logger.info(f"Trade {trade_id} updated with {updates}")

# ----------------------------------------------------------------------------
#       Process Signals from Firebase and Execute Trades for Top Signals
# ----------------------------------------------------------------------------
# Modified section of trade_agent.py to implement portfolio diversification
# Add this function after the KrakenTradingStrategy class

def get_current_portfolio_allocations(kraken_api):
    """
    Retrieve the current portfolio holdings and calculate allocations per asset in USD.
    
    Args:
        kraken_api: Instance of KrakenexAPI
    
    Returns:
        dict: Asset allocations with the amount invested in USD for each asset
    """
    portfolio = {}
    
    # Get account balances
    balances = kraken_api.get_balance()
    logger.info(f"Current balances: {balances}")
    
    # For each asset, convert to USD value
    for asset, amount in balances.items():
        if asset == 'ZUSD':
            continue  # Skip USD itself
            
        try:
            amount = float(amount)
            if amount <= 0:
                continue
                
            # Get the USD trading pair
            pair = f"{asset}USD"
            ticker = kraken_api.get_ticker(pair)
            
            if ticker and 'price' in ticker:
                price = ticker['price']
                usd_value = amount * price
                portfolio[asset] = usd_value
                logger.info(f"Asset {asset}: {amount} worth ${usd_value:.2f}")
            else:
                logger.warning(f"Could not fetch price for {asset}")
        except Exception as e:
            logger.error(f"Error calculating USD value for {asset}: {e}")
    
    return portfolio


# Modify the process_signals_and_trade function to limit investment per coin

def process_signals_and_trade():
    """
    This function retrieves signal documents from Firebase,
    ranks crypto signals that are in uptrend, and attempts to initiate a trade
    for the single most promising crypto signal by placing a buy order.
    The order_watch.py will monitor this order and place the corresponding 
    sell order when filled.
    """
    api_key = settings.KRAKEN_API_KEY
    api_secret = settings.KRAKEN_API_SECRET
    firebase_db = get_firebase_client()
    strategy = KrakenTradingStrategy(api_key, api_secret)
    analyzer = KrakenMarketAnalyzer(api_key, api_secret)
    
    # Define the maximum investment limit per coin in USD
    MAX_INVESTMENT_PER_COIN = 100.0
    
    # Retrieve the current portfolio allocations
    current_portfolio = get_current_portfolio_allocations(analyzer.kraken_api)
    logger.info(f"Current portfolio allocations: {current_portfolio}")

    # Retrieve only the 5 most recent signals from Firebase
    signals_docs = firebase_db.collection('signals').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(5).stream()
    signals_list = []
    for doc in signals_docs:
        signal = doc.to_dict()
        signal['doc_id'] = doc.id
        signals_list.append(signal)

    if not signals_list:
        logger.info("No signals found in Firebase.")
        return "No signals to process"

    # Mark signals as forex if their pair's base is one of the fiat bases
    forex_bases = {"ZEUR", "ZGBP", "ZJPY", "ZCAD", "ZCHF", "ZAUD", "ZNZD"}
    for signal in signals_list:
        pair = signal.get('pair')
        asset_info = strategy._get_pair_details(pair)
        if asset_info and asset_info.get('base') in forex_bases:
            signal['is_forex'] = True
        else:
            signal['is_forex'] = False
        
        # Extract specific strategy signals from the enhanced technical analysis
        signal_details = signal.get('signals', {})
        
        # Check for high-performing strategies from the CMU research
        signal['has_triple_ema'] = signal_details.get('triple_ema_buy', False)
        signal['has_ema_ha_psar'] = signal_details.get('ema_ha_psar_buy', False) 
        signal['has_ttm_squeeze'] = signal_details.get('ttm_squeeze_buy', False)
        
        # Calculate a strategy score based on presence of top strategies
        strategy_score = 0
        if signal.get('has_triple_ema'):  # Highest percentage gain
            strategy_score += 0.3
        if signal.get('has_ema_ha_psar'):  # Best win rate
            strategy_score += 0.5
        if signal.get('has_ttm_squeeze'):  # Most winning trades
            strategy_score += 0.2
            
        # Store the strategy score
        signal['strategy_score'] = strategy_score
        
        # Get composite score that includes news sentiment (if available)
        composite_score = signal.get('composite_score', signal.get('buy_score', 0))
        signal['final_score'] = composite_score * (1 + strategy_score)  # Boost score based on strategy strength

    # Filter for crypto signals only
    crypto_signals = [s for s in signals_list if not s.get('is_forex')]
    
    # Filter for uptrend signals only - check for trend information
    uptrend_crypto_signals = []
    for signal in crypto_signals:
        trend = signal.get('trend', '').lower()
        # Include any signal with "uptrend" in its trend description
        if 'uptrend' in trend:
            uptrend_crypto_signals.append(signal)
    
    if not uptrend_crypto_signals:
        logger.info("No crypto signals in uptrend found.")
        return "No uptrend crypto signals to process"
    
    # Sort the uptrend crypto signals by final score
    uptrend_crypto_signals.sort(key=lambda x: x.get('final_score', 0), reverse=True)
    
    # Filter out signals for coins that already have $100 or more invested
    diversified_signals = []
    for signal in uptrend_crypto_signals:
        pair = signal.get('pair')
        if not pair:
            continue
            
        # Extract the base asset (e.g., XXBT from XXBTZUSD)
        asset_info = analyzer.kraken_api.get_asset_info(pair)
        if not asset_info:
            logger.warning(f"Could not get asset info for {pair}, skipping.")
            continue
            
        base_asset = asset_info.get('base')
        if not base_asset:
            logger.warning(f"Could not determine base asset for {pair}, skipping.")
            continue
            
        # Check current investment in this asset
        current_investment = current_portfolio.get(base_asset, 0)
        logger.info(f"Current investment in {base_asset}: ${current_investment:.2f}")
        
        # Skip if we already have $100 or more invested
        if current_investment >= MAX_INVESTMENT_PER_COIN:
            logger.info(f"Skipping {pair} as we already have ${current_investment:.2f} invested (max: ${MAX_INVESTMENT_PER_COIN:.2f})")
            continue
            
        # If below the limit, add to diversified signals
        signal['current_investment'] = current_investment
        signal['available_investment'] = MAX_INVESTMENT_PER_COIN - current_investment
        diversified_signals.append(signal)
    
    if not diversified_signals:
        logger.info("No eligible signals found after portfolio diversification filter.")
        return "No eligible signals after diversification filter"
    
    # Get only the top signal after diversification
    top_signal = diversified_signals[0] if diversified_signals else None
    
    if not top_signal:
        logger.info("No promising crypto in uptrend found after filtering.")
        return "No suitable signals to process"

    logger.info(f"Processing top crypto signal in uptrend for trade execution: {top_signal.get('pair')} - "
               f"Score: {top_signal.get('final_score', 0):.4f}, "
               f"Trend: {top_signal.get('trend')}, "
               f"Current Investment: ${top_signal.get('current_investment', 0):.2f}, "
               f"Available Investment: ${top_signal.get('available_investment', 0):.2f}, "
               f"Strategies: Triple EMA: {top_signal.get('has_triple_ema')}, "
               f"EMA+HA+PSAR: {top_signal.get('has_ema_ha_psar')}, "
               f"TTM Squeeze: {top_signal.get('has_ttm_squeeze')}")

    # Get account balance
    balances = analyzer.get_balance()
    usd_balance = float(balances.get('ZUSD', 0))
    logger.info(f"Current ZUSD balance: {usd_balance}")
    
    # Set the investment amount based on diversification constraints
    available_investment = top_signal.get('available_investment', MAX_INVESTMENT_PER_COIN)
    
    # Use the smaller value: either the available investment or the default amount
    default_trade_amount = 50.0  # Base amount to trade per signal
    desired_usd = min(available_investment, default_trade_amount)
    
    # Ensure we're investing at least a minimum amount (e.g. $20)
    if desired_usd < 20.0:
        # If we have less than $20 available to invest in this coin, use it all up to reach the $100 cap
        desired_usd = available_investment
    
    logger.info(f"Determined investment amount: ${desired_usd:.2f} for {top_signal.get('pair')}")
    
    # Process the top signal
    pair = top_signal.get('pair')
    current_price = top_signal.get('current_price')
    if not pair or not current_price:
        logger.error("Signal missing pair or current_price, skipping.")
        return "Failed to process signal: missing data"
        
    # Determine trade volume: desired_usd divided by current_price; adjust to minimum order volume if necessary.
    pair_details = strategy._get_pair_details(pair)
    if not pair_details:
        logger.error(f"Could not fetch pair details for {pair}, skipping trade.")
        return f"Failed to fetch details for {pair}"
        
    # Adjust position size based on strategy strength
    position_multiplier = 1.0
    if top_signal.get('has_ema_ha_psar'):  # Best win rate strategy
        position_multiplier = 1.5
    
    # Make sure the multiplier doesn't push us above our available investment
    strategy_adjusted_usd = min(desired_usd * position_multiplier, available_investment)
    
    ordermin = float(pair_details.get('ordermin', 0.0))
    initial_volume = strategy_adjusted_usd / current_price
    trade_volume = initial_volume if initial_volume >= ordermin else ordermin
    
    # Check if this would exceed our diversification limit
    cost_estimate = current_price * trade_volume
    if cost_estimate > available_investment:
        # Adjust volume to fit within available investment
        adjusted_volume = available_investment / current_price
        if adjusted_volume >= ordermin:
            trade_volume = adjusted_volume
            logger.info(f"Adjusted trade volume to {trade_volume} to stay within diversification limit")
        else:
            logger.warning(f"Cannot place order: minimum order size {ordermin} would exceed diversification limit")
            return "Cannot place order: minimum size exceeds diversification limit"
    
    # Check funds (including fee)
    FEE_RATE = 0.0026
    required_cost = current_price * trade_volume * (1 + FEE_RATE)
    if required_cost > usd_balance:
        logger.info(f"Not enough funds for {pair}: requires {required_cost:.8f}, available {usd_balance:.8f}. Skipping.")
        return f"Not enough funds for {pair}"
        
    # Set take profit and stop loss based on strategy performance
    # Default values
    take_profit_pct = 0.02
    stop_loss_pct = 0.02
    
    # Adjust based on strategy - for strategies with higher win rates, we can use tighter stops
    # and for strategies with higher percentage gains, we can set higher take-profit targets
    if top_signal.get('has_triple_ema'):  # Highest percentage gain strategy
        take_profit_pct = 0.03  # More aggressive take profit
        stop_loss_pct = 0.025  # Slightly wider stop loss
    elif top_signal.get('has_ema_ha_psar'):  # Best win rate strategy
        take_profit_pct = 0.018  # More conservative take profit
        stop_loss_pct = 0.015  # Tighter stop loss
    
    tp_price = current_price * (1 + take_profit_pct)
    sl_price = current_price * (1 - stop_loss_pct)
    
    logger.info(f"Attempting trade for {pair}: Buy at {current_price}, TP at {tp_price} ({take_profit_pct*100:.1f}%), "
               f"SL at {sl_price} ({stop_loss_pct*100:.1f}%), volume {trade_volume}")
               
    # Execute trade with the adjusted parameters - now just initiates the buy order
    trade_id = execute_trade_with_adjustable_sl_tp(
        strategy=strategy,
        pair=pair,
        buy_price=current_price,
        tp_price=tp_price,
        sl_price=sl_price,
        volume=trade_volume
    )
    
    if trade_id:
        logger.info(f"Trade initiated for {pair} with ID: {trade_id}")
        # Update signal document to mark as processed
        firebase_db.collection('signals').document(top_signal['doc_id']).update({
            'trade_executed': True,
            'executed_at': datetime.utcnow().isoformat(),
            'trade_id': trade_id,
            'investment_amount': current_price * trade_volume,
            'diversification_applied': True,
            'strategy_used': ', '.join(filter(None, [
                'Triple EMA' if top_signal.get('has_triple_ema') else None,
                'EMA+HA+PSAR' if top_signal.get('has_ema_ha_psar') else None,
                'TTM Squeeze' if top_signal.get('has_ttm_squeeze') else None
            ]))
        })
        return f"Successfully initiated trade for {pair}"
    else:
        logger.error(f"Failed to initiate trade for {pair}.")
        return f"Failed to initiate trade for {pair}"

def execute_trade_with_adjustable_sl_tp(strategy, pair, buy_price, tp_price, sl_price, volume):
    """
    Helper function to execute a trade with configurable take profit and stop loss prices.
    Places only the buy order and stores trade information for later monitoring.
    
    Args:
        strategy: KrakenTradingStrategy instance
        pair: Trading pair symbol
        buy_price: Entry price
        tp_price: Take profit price
        sl_price: Stop loss price
        volume: Trade volume
        
    Returns:
        Trade ID if successful, None if failed
    """
    pair_info = strategy._get_pair_details(pair)
    if not pair_info:
        logger.error(f"Could not get pair info for {pair}")
        return None
        
    pair_decimals = int(pair_info.get('pair_decimals', 5))
    lot_decimals = int(pair_info.get('lot_decimals', 8))
    ordermin = float(pair_info.get('ordermin', 0.0))
    
    if volume < ordermin:
        logger.warning(f"Volume {volume} below minimum {ordermin} for {pair}. Adjusting to minimum volume.")
        volume = ordermin
        
    formatted_buy_price = strategy._format_decimal(buy_price, pair_decimals)
    formatted_tp_price = strategy._format_decimal(tp_price, pair_decimals)
    formatted_sl_price = strategy._format_decimal(sl_price, pair_decimals)
    formatted_volume = strategy._format_decimal(volume, lot_decimals)
    
    # Place buy order
    buy_order = strategy.api.query_private("AddOrder", {
        "pair": pair,
        "type": "buy",
        "ordertype": "limit",
        "price": str(formatted_buy_price),
        "volume": str(formatted_volume)
    })
    
    if buy_order.get('error'):
        logger.error(f"Error placing BUY order: {buy_order['error']}")
        return None
        
    buy_txid = list(buy_order["result"]["txid"])[0]
    logger.info(f"Buy order placed for {pair}, txid: {buy_txid}")
    
    # Store comprehensive trade information for the order_watch to use
    trade_record = {
        'pair': pair,
        'buy_order_id': buy_txid,
        'buy_price': formatted_buy_price,
        'tp_price': formatted_tp_price,
        'sl_price': formatted_sl_price,
        'volume': formatted_volume,
        'order_ids': {'buy': buy_txid},
        'status': 'pending',
        'created_at': datetime.utcnow().isoformat()
    }
    
    trade_id = strategy.store_trade(trade_record)
    logger.info(f"Trade initiated with ID {trade_id} for {pair}. Buy order {buy_txid} placed.")
    
    return trade_id
# ----------------------------------------------------------------------------
#                             MAIN EXECUTION
# ----------------------------------------------------------------------------
@shared_task
def portfolio_values():
    api_key = settings.KRAKEN_API_KEY
    api_secret = settings.KRAKEN_API_SECRET
    analyzer = KrakenMarketAnalyzer(api_key, api_secret)
    balances = analyzer.get_balance()
    total_value = 0
    print("\n--- Portfolio Value ---")
    for asset, amount in balances.items():
        amount = float(amount)
        if asset == 'ZUSD':
            value = amount
        else:
            pair = f"{asset}USD"
            ticker = analyzer.kraken_api.get_ticker(pair)
            if ticker:
                price = ticker.get('price', 0)
                value = amount * price
            else:
                value = 0
        print(f"{asset}: {amount:.8f} (${value:.2f})")
        total_value += value
    print(f"\nTotal Current Portfolio Value: ${total_value:.2f}")
    return total_value

# ----------------------------------------------------------------------------
#                  Django Command to Start the Bot
# ----------------------------------------------------------------------------
class Command(BaseCommand):
    help = 'Retrieves signals from Firebase, ranks them, and executes trades for the top signals.'

    def handle(self, *args, **options):
        from django_celery_beat.models import PeriodicTask, IntervalSchedule
        logger.info("Starting signal processing and trade execution process...")
        result = process_signals_and_trade()
        logger.info(result)
        # Optionally, you may also call portfolio_values() to log current portfolio value.
        portfolio_values()
