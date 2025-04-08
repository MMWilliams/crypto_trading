# apps/market/management/commands/order_watch.py
# this commadn monitors buy orders for exscution for placing subsequent sell ordors, or cancels buy orders 
# pip install krakenex python-dotenv celery firebase-admin schedule


# The code implements an automated trading bot that monitors pending cryptocurrency buy orders placed on the Kraken exchange, automatically placing sell orders (take-profit and stop-loss) once a buy order is filled or canceling stale orders after six hours. Additionally, it periodically calculates and logs the total value of the user's portfolio based on current market prices retrieved from Kraken.

import krakenex
import requests
import pandas as pd
import numpy as np
import time
import logging
import threading  # For concurrent order monitoring
import uuid       # To generate unique trade IDs
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
#               Market Analyzer for Illustrations
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
        self.kraken_api = KrakenexAPI(api_key, api_secret)

    def _rate_limited_request(self, endpoint: str, params: Dict = None) -> Dict:
        now_time = time.time()
        time_since_last = now_time - self.last_request_time
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
        top_pairs = df.nsmallest(10, 'combined_rank')['pair'].tolist()
        self.logger.info(f"Top pairs for analysis: {top_pairs}")
        return top_pairs

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        low_min = df['low'].rolling(14).min()
        high_max = df['high'].rolling(14).max()
        df['stoch_k'] = 100 * ((df['close'] - low_min) / (high_max - low_min))
        df['stoch_d'] = df['stoch_k'].rolling(3).mean()
        return df

    def analyze_signals(self, df: pd.DataFrame) -> Dict[str, bool]:
        current = df.iloc[-1]
        previous = df.iloc[-2]
        signals = {
            'rsi_oversold': current['rsi'] < 30,
            'macd_crossover': current['macd'] > current['macd_signal'] and previous['macd'] <= previous['macd_signal'],
            'ema_crossover': current['ema_9'] > current['ema_20'] and previous['ema_9'] <= previous['ema_20'],
            'stoch_oversold_cross': current['stoch_k'] > current['stoch_d'] and previous['stoch_k'] <= previous['stoch_d'] and current['stoch_k'] < 30
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
        return {
            'pair': pair,
            'current_price': current_price,
            'trend': trend,
            'signals': signals,
            'buy_score': signals.get('buy_score', 0),
            'indicators': indicators
        }

# ----------------------------------------------------------------------------
#               Trading Logic using krakenex (Order Execution)
# ----------------------------------------------------------------------------
# apps/market/management/commands/order_watch.py
# This command monitors buy orders for execution and places subsequent sell orders or cancels stale buy orders
# pip install krakenex python-dotenv celery firebase-admin schedule

import krakenex
import requests
import pandas as pd
import numpy as np
import time
import logging
import uuid
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_DOWN
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

# Import existing classes - KrakenexAPI and KrakenMarketAnalyzer
# (keeping all the existing code from these classes)
# ...

class KrakenTradingStrategy:
    """
    This class integrates Firebase to store trade records.
    The monitor_pending_trades method regularly checks for filled buy orders
    and places corresponding sell orders, or cancels stale orders.
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

    def _get_pair_details(self, pair: str) -> dict:
        resp = self.api.query_public("AssetPairs", {"pair": pair})
        if resp.get("error"):
            self.logger.error(f"Error fetching asset info for pair={pair}: {resp['error']}")
            return {}
        return resp.get("result", {}).get(pair, {})

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

    def place_sell_orders_for_trade(self, trade_id: str, trade_record: dict) -> Tuple[str, str]:
        """
        Places take-profit and stop-loss sell orders once a buy order is filled.
        Uses target prices from the trade record if available, otherwise uses default values.
        """
        pair = trade_record.get('pair')
        buy_price = float(trade_record.get('buy_price'))
        volume = float(trade_record.get('volume'))
        
        # Use TP/SL prices from trade record if available, otherwise calculate defaults
        if 'tp_price' in trade_record:
            tp_price = float(trade_record['tp_price'])
        else:
            tp_price = buy_price * 1.02  # Default 2% profit
            
        if 'sl_price' in trade_record:
            stop_price = float(trade_record['sl_price'])
        else:
            stop_price = buy_price * 0.98  # Default 2% stop loss

        pair_info = self._get_pair_details(pair)
        if not pair_info:
            self.logger.error(f"Unable to fetch pair details for {pair} when placing sell orders.")
            return None, None
            
        pair_decimals = int(pair_info.get('pair_decimals', 5))
        lot_decimals = int(pair_info.get('lot_decimals', 8))
        formatted_tp_price = self._format_decimal(tp_price, pair_decimals)
        formatted_stop_price = self._format_decimal(stop_price, pair_decimals)
        formatted_volume = self._format_decimal(volume, lot_decimals)

        # Place TP SELL order
        tp_sell_order = self.api.query_private("AddOrder", {
            "pair": pair,
            "type": "sell",
            "ordertype": "take-profit-limit",
            "price": str(formatted_tp_price),
            "price2": str(formatted_tp_price),
            "volume": str(formatted_volume)
        })
        
        if tp_sell_order.get('error'):
            self.logger.error(f"Error placing TP SELL order for {pair}: {tp_sell_order['error']}")
            return None, None
            
        tp_sell_txid = list(tp_sell_order["result"]["txid"])[0]
        
        # Place SL SELL order
        sl_sell_order = self.api.query_private("AddOrder", {
            "pair": pair,
            "type": "sell",
            "ordertype": "stop-loss",
            "price": str(formatted_stop_price),
            "volume": str(formatted_volume)
        })
        
        if sl_sell_order.get('error'):
            self.logger.error(f"Error placing SL SELL order for {pair}: {sl_sell_order['error']}")
            # Cancel the take-profit order if we can't place the stop-loss
            self.cancel_order(tp_sell_txid)
            return None, None
            
        sl_sell_txid = list(sl_sell_order["result"]["txid"])[0]

        # Update trade record
        updated_order_ids = {
            'buy': trade_record['order_ids']['buy'],
            'tp': tp_sell_txid,
            'sl': sl_sell_txid
        }
        
        self.firebase_db.collection('trades').document(trade_id).update({
            'order_ids': updated_order_ids,
            'status': 'active',
            'tp_order_id': tp_sell_txid,
            'sl_order_id': sl_sell_txid,
            'tp_price_final': formatted_tp_price,
            'sl_price_final': formatted_stop_price,
            'filled_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        })
        
        self.logger.info(f"Sell orders placed for {pair}: TP {tp_sell_txid}, SL {sl_sell_txid}")
        return tp_sell_txid, sl_sell_txid

    def monitor_pending_trades(self):
        """
        For each trade with status 'pending', checks if:
        - The buy order is filled (closed) -> places sell orders
        - The trade has been pending too long -> cancels the buy order
        Designed to be called periodically as a Celery task.
        """
        logger.info("Running monitor_pending_trades...")
        
        current_time = datetime.utcnow()
        max_pending_time = 21600  # 6 hours in seconds
        
        # Get all pending trades
        try:
            trades = self.firebase_db.collection('trades').where('status', '==', 'pending').stream()
            pending_trades = list(trades)  # Convert to list to check if empty
            
            if not pending_trades:
                logger.info("No pending trades found.")
                return "No pending trades to monitor"
                
            logger.info(f"Found {len(pending_trades)} pending trades to check.")
            
            for doc in pending_trades:
                trade_id = doc.id
                trade = doc.to_dict()
                logger.info(f"Checking trade {trade_id}")
                
                # Get buy order ID
                buy_order_id = trade.get('order_ids', {}).get('buy')
                if not buy_order_id:
                    logger.error(f"Trade {trade_id} has no buy order ID. Marking as error.")
                    self.firebase_db.collection('trades').document(trade_id).update({
                        'status': 'error',
                        'error_message': 'Missing buy order ID',
                        'updated_at': current_time.isoformat()
                    })
                    continue
                
                # Check order status
                status = self.check_order_status(buy_order_id)
                logger.info(f"Buy order {buy_order_id} status: {status}")
                
                # If order is filled, place sell orders
                if status == 'closed':
                    logger.info(f"Buy order {buy_order_id} is filled. Placing sell orders.")
                    tp_sell_id, sl_sell_id = self.place_sell_orders_for_trade(trade_id, trade)
                    
                    if not tp_sell_id or not sl_sell_id:
                        logger.error(f"Failed to place one or both sell orders for {trade_id}.")
                        self.firebase_db.collection('trades').document(trade_id).update({
                            'status': 'error',
                            'error_message': 'Failed to place sell orders',
                            'updated_at': current_time.isoformat()
                        })
                
                # Check if order has been pending too long
                elif status in ['pending', 'open']:
                    created_at = None
                    if isinstance(trade.get('created_at'), str):
                        try:
                            created_at = datetime.fromisoformat(trade['created_at'])
                        except ValueError:
                            # Handle case where isoformat might not be compatible
                            created_at = None
                    else:
                        created_at = trade.get('created_at')
                    
                    if created_at:
                        elapsed_seconds = (current_time - created_at).total_seconds()
                        
                        if elapsed_seconds > max_pending_time:
                            logger.info(f"Buy order {buy_order_id} has been pending for over 6 hours. Cancelling.")
                            cancelled = self.cancel_order(buy_order_id)
                            
                            if cancelled:
                                self.firebase_db.collection('trades').document(trade_id).update({
                                    'status': 'cancelled',
                                    'cancelled_at': current_time.isoformat(),
                                    'cancel_reason': 'timeout',
                                    'updated_at': current_time.isoformat()
                                })
                            else:
                                logger.error(f"Failed to cancel order {buy_order_id}.")
                    else:
                        logger.warning(f"Trade {trade_id} has no valid creation timestamp, cannot determine age.")
                
                # Handle other statuses (canceled, expired, etc.)
                elif status in ['canceled', 'expired']:
                    logger.info(f"Buy order {buy_order_id} is {status}. Updating trade record.")
                    self.firebase_db.collection('trades').document(trade_id).update({
                        'status': 'cancelled',
                        'cancelled_at': current_time.isoformat(),
                        'cancel_reason': f'Order was {status}',
                        'updated_at': current_time.isoformat()
                    })
                else:
                    logger.warning(f"Unhandled order status '{status}' for buy order {buy_order_id}.")
            
            return f"Processed {len(pending_trades)} pending trades."
            
        except Exception as e:
            logger.error(f"Error in monitor_pending_trades: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

# ----------------------------------------------------------------------------
#                             Celery Tasks
# ----------------------------------------------------------------------------
@shared_task
def monitor_orders():
    """
    Celery task to monitor pending orders and act on them.
    Runs as a scheduled task every 5 minutes.
    """
    logger.info("Starting scheduled order monitoring...")
    api_key = settings.KRAKEN_API_KEY
    api_secret = settings.KRAKEN_API_SECRET
    strategy = KrakenTradingStrategy(api_key, api_secret)
    result = strategy.monitor_pending_trades()
    logger.info(f"Completed order monitoring: {result}")
    return result

@shared_task
def portfolio_values():
    """
    Celery task to calculate and log the current portfolio value.
    """
    api_key = settings.KRAKEN_API_KEY
    api_secret = settings.KRAKEN_API_SECRET
    analyzer = KrakenMarketAnalyzer(api_key, api_secret)
    balances = analyzer.get_balance()
    total_value = 0
    logger.info("\n--- Portfolio Value ---")
    
    for asset, amount in balances.items():
        amount = float(amount)
        if asset == 'ZUSD':
            value = amount
        else:
            pair = f"{asset}USD"
            ticker = analyzer.kraken_api.get_ticker(pair)
            price = ticker.get('price', 0) if ticker else 0
            value = amount * price
        logger.info(f"{asset}: {amount:.8f} (${value:.2f})")
        total_value += value
    
    logger.info(f"\nTotal Current Portfolio Value: ${total_value:.2f}")
    return total_value

# ----------------------------------------------------------------------------
#                  Django Command to Start the Bot
# ----------------------------------------------------------------------------
class Command(BaseCommand):
    help = 'Monitors pending crypto trades and handles filled orders or cancellations.'

    def handle(self, *args, **options):
        logger.info("Initiating order monitoring process...")
        api_key = settings.KRAKEN_API_KEY
        api_secret = settings.KRAKEN_API_SECRET
        strategy = KrakenTradingStrategy(api_key, api_secret)
        
        # Run once immediately
        result = strategy.monitor_pending_trades()
        logger.info(f"Initial monitoring result: {result}")
        
        # Also log current portfolio value
        portfolio_values()
        
        # Note: we don't start a thread here - Celery will schedule the task
        self.stdout.write(self.style.SUCCESS('Order monitoring completed. Periodic monitoring will be handled by Celery tasks.'))