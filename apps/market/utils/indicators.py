# apps/market/utils/indicators.py

import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class TechnicalIndicators:
    """Technical analysis indicators for cryptocurrency"""
    symbol: str
    current_price: float
    timestamp: int
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    rsi_14: Optional[float] = None
    bollinger_upper: Optional[float] = None
    bollinger_middle: Optional[float] = None
    bollinger_lower: Optional[float] = None
    volume_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    volatility_24h: Optional[float] = None

def calculate_indicators(historical_prices: List[Dict]) -> TechnicalIndicators:
    """Calculate technical indicators from historical price data"""
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(historical_prices)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.set_index('timestamp').sort_index()
    
    # Get current price info
    current_price = df['price_usd'].iloc[-1]
    current_timestamp = int(df.index[-1].timestamp())
    
    try:
        # Simple Moving Averages
        sma_20 = df['price_usd'].rolling(window=20).mean().iloc[-1]
        sma_50 = df['price_usd'].rolling(window=50).mean().iloc[-1]
        sma_200 = df['price_usd'].rolling(window=200).mean().iloc[-1]
        
        # Exponential Moving Averages for MACD
        ema_12 = df['price_usd'].ewm(span=12, adjust=False).mean()
        ema_26 = df['price_usd'].ewm(span=26, adjust=False).mean()
        
        # MACD
        macd = ema_12 - ema_26
        macd_signal = macd.ewm(span=9, adjust=False).mean()
        macd_histogram = macd - macd_signal
        
        # RSI
        delta = df['price_usd'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi_14 = 100 - (100 / (1 + rs)).iloc[-1]
        
        # Bollinger Bands
        rolling_mean = df['price_usd'].rolling(window=20).mean()
        rolling_std = df['price_usd'].rolling(window=20).std()
        bollinger_upper = rolling_mean + (rolling_std * 2)
        bollinger_lower = rolling_mean - (rolling_std * 2)
        
        # 24h Changes
        price_24h_ago = df['price_usd'].shift(24)
        price_change_24h = ((current_price - price_24h_ago) / price_24h_ago * 100).iloc[-1]
        
        # 24h Volatility (standard deviation of returns)
        returns = df['price_usd'].pct_change()
        volatility_24h = returns.rolling(window=24).std().iloc[-1] * 100
        
        return TechnicalIndicators(
            symbol=df['symbol'].iloc[-1],
            current_price=current_price,
            timestamp=current_timestamp,
            sma_20=sma_20,
            sma_50=sma_50,
            sma_200=sma_200,
            ema_12=ema_12.iloc[-1],
            ema_26=ema_26.iloc[-1],
            macd=macd.iloc[-1],
            macd_signal=macd_signal.iloc[-1],
            macd_histogram=macd_histogram.iloc[-1],
            rsi_14=rsi_14,
            bollinger_upper=bollinger_upper.iloc[-1],
            bollinger_middle=rolling_mean.iloc[-1],
            bollinger_lower=bollinger_lower.iloc[-1],
            price_change_24h=price_change_24h,
            volatility_24h=volatility_24h
        )
        
    except Exception as e:
        print(f"Error calculating indicators: {e}")
        return TechnicalIndicators(
            symbol=df['symbol'].iloc[-1],
            current_price=current_price,
            timestamp=current_timestamp
        )

def generate_market_signals(indicators: TechnicalIndicators) -> Dict:
    """Generate trading signals based on technical indicators"""
    
    signals = {
        'trend': {
            'short_term': 'neutral',
            'medium_term': 'neutral',
            'long_term': 'neutral'
        },
        'signals': [],
        'strength': 0  # -100 to 100
    }
    
    try:
        # Trend Analysis
        if indicators.sma_20 and indicators.sma_50 and indicators.sma_200:
            # Short-term trend (20 SMA vs current price)
            if indicators.current_price > indicators.sma_20:
                signals['trend']['short_term'] = 'bullish'
            else:
                signals['trend']['short_term'] = 'bearish'
                
            # Medium-term trend (50 SMA vs current price)
            if indicators.current_price > indicators.sma_50:
                signals['trend']['medium_term'] = 'bullish'
            else:
                signals['trend']['medium_term'] = 'bearish'
                
            # Long-term trend (200 SMA vs current price)
            if indicators.current_price > indicators.sma_200:
                signals['trend']['long_term'] = 'bullish'
            else:
                signals['trend']['long_term'] = 'bearish'
        
        # MACD Signal
        if indicators.macd is not None and indicators.macd_signal is not None:
            if indicators.macd > indicators.macd_signal:
                signals['signals'].append('MACD bullish crossover')
            elif indicators.macd < indicators.macd_signal:
                signals['signals'].append('MACD bearish crossover')
        
        # RSI Signals
        if indicators.rsi_14 is not None:
            if indicators.rsi_14 > 70:
                signals['signals'].append('RSI overbought')
            elif indicators.rsi_14 < 30:
                signals['signals'].append('RSI oversold')
        
        # Bollinger Bands Signals
        if all([indicators.bollinger_upper, indicators.bollinger_lower]):
            if indicators.current_price > indicators.bollinger_upper:
                signals['signals'].append('Price above upper Bollinger Band')
            elif indicators.current_price < indicators.bollinger_lower:
                signals['signals'].append('Price below lower Bollinger Band')
        
        # Calculate overall signal strength
        strength = 0
        if signals['trend']['short_term'] == 'bullish': strength += 20
        if signals['trend']['medium_term'] == 'bullish': strength += 30
        if signals['trend']['long_term'] == 'bullish': strength += 50
        if signals['trend']['short_term'] == 'bearish': strength -= 20
        if signals['trend']['medium_term'] == 'bearish': strength -= 30
        if signals['trend']['long_term'] == 'bearish': strength -= 50
        
        signals['strength'] = max(min(strength, 100), -100)
        
        return signals
        
    except Exception as e:
        print(f"Error generating signals: {e}")
        return signals