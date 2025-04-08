# apps/market/management/commands/close_positions.py
# this command sells all coins and cancels existing buy orders,
# or, if run with --sell-all, checks balances and sells all coins.
# pip install krakenex python-dotenv celery firebase-admin schedule

# This script cancels all existing open buy orders and sells every cryptocurrency asset in the user's Kraken portfolio (excluding USD) via market orders. Additionally, it calculates and logs the current total portfolio value after liquidating the positions.
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

    def execute_trade(self, pair: str, buy_price: float, sell_price: float, volume: float,
                      max_wait: int = 21600, poll_interval: int = 5) -> Tuple[str, str, str]:
        pair_info = self._get_pair_details(pair)
        if not pair_info:
            self.logger.error(f"Could not get pair info for {pair}")
            return None, None, None
        pair_decimals = int(pair_info.get('pair_decimals', 5))
        lot_decimals = int(pair_info.get('lot_decimals', 8))
        ordermin = float(pair_info.get('ordermin', 0.0))
        if volume < ordermin:
            self.logger.warning(f"Volume {volume} below minimum {ordermin} for {pair}. Adjusting to minimum volume.")
            volume = ordermin
        formatted_buy_price = self._format_decimal(buy_price, pair_decimals)
        formatted_sell_price = self._format_decimal(sell_price, pair_decimals)
        formatted_volume = self._format_decimal(volume, lot_decimals)
        buy_order = self.api.query_private("AddOrder", {
            "pair": pair,
            "type": "buy",
            "ordertype": "limit",
            "price": str(formatted_buy_price),
            "volume": str(formatted_volume)
        })
        if buy_order.get('error'):
            self.logger.error(f"Error placing BUY order: {buy_order['error']}")
            return None, None, None
        buy_txid = list(buy_order["result"]["txid"])[0]
        self.logger.info(f"Buy order placed for {pair}, txid: {buy_txid}")
        trade_record = {
            'pair': pair,
            'buy_order_id': buy_txid,
            'buy_price': formatted_buy_price,
            'volume': volume,
            'order_ids': {'buy': buy_txid},
            'status': 'pending'
        }
        trade_id = self.store_trade(trade_record)
        start_time = time.time()
        filled = False
        while time.time() - start_time < max_wait:
            status = self.check_order_status(buy_txid)
            if status == "closed":
                filled = True
                self.logger.info(f"Buy order {buy_txid} filled.")
                break
            elif status is None:
                self.logger.warning(f"Order status for {buy_txid} is None, retrying.")
            else:
                self.logger.info(f"Buy order {buy_txid} status: {status}. Waiting...")
            time.sleep(poll_interval)
        if not filled:
            self.logger.error(f"Buy order {buy_txid} was not filled within {max_wait} seconds. Cancelling order.")
            self.cancel_order(buy_txid)
            self.update_trade(trade_id, {'status': 'cancelled'})
            return None, None, None
        stop_price = float(formatted_buy_price) * 0.98
        formatted_stop_price = self._format_decimal(stop_price, pair_decimals)
        tp_sell_order = self.api.query_private("AddOrder", {
            "pair": pair,
            "type": "sell",
            "ordertype": "take-profit-limit",
            "price": str(formatted_sell_price),
            "price2": str(formatted_sell_price),
            "volume": str(formatted_volume)
        })
        if tp_sell_order.get('error'):
            self.logger.error(f"Error placing TP SELL order: {tp_sell_order['error']}. Cancelling buy order.")
            self.cancel_order(buy_txid)
            self.update_trade(trade_id, {'status': 'cancelled'})
            return buy_txid, None, None
        tp_sell_txid = list(tp_sell_order["result"]["txid"])[0]
        sl_sell_order = self.api.query_private("AddOrder", {
            "pair": pair,
            "type": "sell",
            "ordertype": "stop-loss",
            "price": str(formatted_stop_price),
            "volume": str(formatted_volume)
        })
        if sl_sell_order.get('error'):
            self.logger.error(f"Error placing SL SELL order: {sl_sell_order['error']}. Cancelling orders.")
            self.cancel_order(buy_txid)
            self.cancel_order(tp_sell_txid)
            self.update_trade(trade_id, {'status': 'cancelled'})
            return buy_txid, tp_sell_txid, None
        sl_sell_txid = list(sl_sell_order["result"]["txid"])[0]
        updated_order_ids = {'buy': buy_txid, 'tp': tp_sell_txid, 'sl': sl_sell_txid}
        self.update_trade(trade_id, {'order_ids': updated_order_ids, 'status': 'active'})
        self.logger.info(f"Trade executed for {pair}: Buy order filled (txid={buy_txid}), TP SELL (txid={tp_sell_txid}), SL SELL (txid={sl_sell_txid}).")
        return buy_txid, tp_sell_txid, sl_sell_txid

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

    # ------------------------------------------------------------------------
    # New: Sell an asset via a market order (for "sell all" functionality)
    # ------------------------------------------------------------------------
    def sell_asset(self, asset: str, pair: str, volume: float):
        pair_info = self._get_pair_details(pair)
        if not pair_info:
            self.logger.error(f"Could not fetch pair details for {pair}")
            return None
        pair_decimals = int(pair_info.get('pair_decimals', 5))
        lot_decimals = int(pair_info.get('lot_decimals', 8))
        ordermin = float(pair_info.get('ordermin', 0.0))
        if volume < ordermin:
            self.logger.warning(f"Volume {volume} is below minimum order volume {ordermin} for {pair}. Skipping sale.")
            return None
        formatted_volume = self._format_decimal(volume, lot_decimals)
        sell_order = self.api.query_private("AddOrder", {
            "pair": pair,
            "type": "sell",
            "ordertype": "market",
            "volume": str(formatted_volume)
        })
        if sell_order.get("error"):
            self.logger.error(f"Error placing market sell order for {asset} on {pair}: {sell_order['error']}")
            return None
        txid = list(sell_order["result"]["txid"])[0]
        self.logger.info(f"Market sell order placed for {asset} on {pair}, txid: {txid}")
        return txid

    def cancel_all_open_buy_orders(self):
        open_orders = self.api.query_private("OpenOrders", {})
        if open_orders.get("error"):
            self.logger.error(f"Error fetching open orders: {open_orders['error']}")
            return
        orders = open_orders.get("result", {}).get("open", {})
        for order_id, order in orders.items():
            order_type = order.get("descr", {}).get("type", "").lower()
            if order_type == "buy":
                self.logger.info(f"Cancelling open buy order {order_id}")
                self.cancel_order(order_id)
    def get_pair_for_asset(self, asset: str) -> str:
        """
        Returns the Kraken asset pair for the given asset that trades against USD.
        This function queries Kraken's AssetPairs endpoint and searches for a pair where the quote is 'ZUSD'
        and the base contains the asset string.
        """
        resp = self.api.query_public("AssetPairs", {})
        if resp.get("error"):
            self.logger.error(f"Error fetching asset pairs: {resp['error']}")
            return None
        pairs = resp.get("result", {})
        for pair_name, pair_info in pairs.items():
            # Check if the quote is ZUSD and the base matches the asset (this may need adjustment based on your account's naming)
            if pair_info.get("quote") == "ZUSD" and asset.upper() in pair_info.get("base", "").upper():
                return pair_name
        self.logger.error(f"No trading pair found for asset {asset} against USD")
        return None
# ----------------------------------------------------------------------------
#       Process Signals from Firebase and Execute Trades for Top Signals
# ----------------------------------------------------------------------------
def process_signals_and_trade():
    """
    This function retrieves signal documents from Firebase,
    ranks them with a preference for forex signals, and attempts to execute trades
    for up to the top 5 signals if there are sufficient funds.
    """
    api_key = settings.KRAKEN_API_KEY
    api_secret = settings.KRAKEN_API_SECRET
    firebase_db = get_firebase_client()
    strategy = KrakenTradingStrategy(api_key, api_secret)
    analyzer = KrakenMarketAnalyzer(api_key, api_secret)

    # Retrieve all signals from Firebase
    signals_docs = firebase_db.collection('signals').stream()
    signals_list = []
    for doc in signals_docs:
        signal = doc.to_dict()
        signal['doc_id'] = doc.id
        signals_list.append(signal)

    if not signals_list:
        logger.info("No signals found in Firebase.")
        return "No signals to process"

    # Mark signals as forex if their pair’s base is one of the fiat bases
    forex_bases = {"ZEUR", "ZGBP", "ZJPY", "ZCAD", "ZCHF", "ZAUD", "ZNZD"}
    for signal in signals_list:
        pair = signal.get('pair')
        asset_info = strategy._get_pair_details(pair)
        if asset_info and asset_info.get('base') in forex_bases:
            signal['is_forex'] = True
        else:
            signal['is_forex'] = False

    # Rank signals: sort forex signals by buy_score descending, then crypto signals by buy_score descending,
    # and combine forex signals first.
    forex_signals = [s for s in signals_list if s.get('is_forex')]
    crypto_signals = [s for s in signals_list if not s.get('is_forex')]
    forex_signals.sort(key=lambda x: x.get('buy_score', 0), reverse=True)
    crypto_signals.sort(key=lambda x: x.get('buy_score', 0), reverse=True)
    ranked_signals = forex_signals + crypto_signals
    top_signals = ranked_signals[:5]
    logger.info(f"Processing top {len(top_signals)} signals for trade execution.")

    # Get account balance
    balances = analyzer.get_balance()
    usd_balance = float(balances.get('ZUSD', 0))
    logger.info(f"Current ZUSD balance: {usd_balance}")
    desired_usd = 5.0  # Amount to trade per signal

    # Process each signal in order
    for signal in top_signals:
        pair = signal.get('pair')
        current_price = signal.get('current_price')
        if not pair or not current_price:
            logger.error("Signal missing pair or current_price, skipping.")
            continue
        # Determine trade volume: desired_usd divided by current_price; adjust to minimum order volume if necessary.
        pair_details = strategy._get_pair_details(pair)
        if not pair_details:
            logger.error(f"Could not fetch pair details for {pair}, skipping trade.")
            continue
        ordermin = float(pair_details.get('ordermin', 0.0))
        initial_volume = desired_usd / current_price
        trade_volume = initial_volume if initial_volume >= ordermin else ordermin
        # Check funds (including fee)
        FEE_RATE = 0.0026
        required_cost = current_price * trade_volume * (1 + FEE_RATE)
        if required_cost > usd_balance:
            logger.info(f"Not enough funds for {pair}: requires {required_cost:.8f}, available {usd_balance:.8f}. Skipping.")
            continue
        # Set take profit price (e.g., 2% profit)
        take_profit_pct = 0.02
        tp_price = current_price * (1 + take_profit_pct)
        logger.info(f"Attempting trade for {pair}: Buy at {current_price}, TP at {tp_price}, volume {trade_volume}")
        # Execute trade
        buy_id, tp_sell_id, sl_sell_id = strategy.execute_trade(
            pair=pair,
            buy_price=current_price,
            sell_price=tp_price,
            volume=trade_volume
        )
        if buy_id and tp_sell_id and sl_sell_id:
            logger.info(f"Trade executed for {pair}: BUY {buy_id}, TP SELL {tp_sell_id}, SL SELL {sl_sell_id}")
            # Update signal document to mark as processed
            firebase_db.collection('signals').document(signal['doc_id']).update({
                'trade_executed': True,
                'executed_at': datetime.utcnow().isoformat()
            })
        else:
            logger.error(f"Trade execution failed for {pair}.")
    return f"Processed {len(top_signals)} signals."

    
# ----------------------------------------------------------------------------
#       New: Sell All Coins Functionality
# ----------------------------------------------------------------------------
def sell_all_coins():
    """
    This function retrieves the current balances and, for every asset (except ZUSD),
    cancels all open buy orders and places a market sell order for the entire balance.
    """
    api_key = settings.KRAKEN_API_KEY
    api_secret = settings.KRAKEN_API_SECRET
    strategy = KrakenTradingStrategy(api_key, api_secret)

    # Cancel all open buy orders before selling coins.
    strategy.logger.info("Cancelling all open buy orders...")
    strategy.cancel_all_open_buy_orders()

    balances = strategy._get_balance()
    for asset, amount_str in balances.items():
        if asset == 'ZUSD':
            continue  # Skip USD balance
        try:
            volume = float(amount_str)
        except ValueError:
            strategy.logger.error(f"Invalid balance for {asset}: {amount_str}")
            continue
        if volume <= 0:
            continue

        # Instead of constructing the pair by asset + "ZUSD", use the helper function:
        pair = strategy.get_pair_for_asset(asset)
        if not pair:
            strategy.logger.error(f"Could not determine trading pair for asset {asset}, skipping sale.")
            continue

        strategy.logger.info(f"Attempting to sell {volume} of {asset} on pair {pair}")
        txid = strategy.sell_asset(asset, pair, volume)
        if txid:
            strategy.logger.info(f"Successfully placed sell order for {asset} with txid: {txid}")
        else:
            strategy.logger.error(f"Failed to sell {asset}")
    return "Sell all coins executed."

# ----------------------------------------------------------------------------
#                             Portfolio Value Task
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
            pair = f"{asset}USD"  # Note: Adjust if your Kraken asset pairs differ.
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
    help = 'Retrieves signals from Firebase and executes trades for the top signals, or sells all coins if --sell-all is provided.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--sell-all',
            action='store_true',
            help='Sell all coins in the portfolio'
        )

    def handle(self, *args, **options):
            logger.info("Sell all coins command triggered.")
            result = sell_all_coins()
            logger.info(result)
        # if options.get('sell_all'):
        #     logger.info("Sell all coins command triggered.")
        #     result = sell_all_coins()
        #     logger.info(result)
        # else:
        #     logger.info("Starting signal processing and trade execution process...")
        #     result = process_signals_and_trade()
        #     logger.info(result)
        #     # Optionally, you may also call portfolio_values() to log current portfolio value.
        #     portfolio_values()
