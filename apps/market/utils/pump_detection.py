# apps/market/utils/pump_detection.py

from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

@dataclass
class PumpAnalysis:
    symbol: str
    is_pump: bool
    pump_score: float  # 0-100 scale indicating pump likelihood
    price_increase_pct: float
    time_frame: str  # 'minutes', 'hours', 'days'
    initial_price: float
    peak_price: float
    time_to_peak: float  # in minutes
    volume_increase_pct: float
    is_suspicious: bool
    risk_level: str  # 'low', 'medium', 'high', 'extreme'
    warning_signs: List[str]

def detect_pump(historical_prices: List[Dict], min_price_threshold: float = 0.01) -> PumpAnalysis:
    """
    Detect if a coin is being "pumped" based on price and volume patterns
    
    Args:
        historical_prices: List of historical price data
        min_price_threshold: Minimum price to consider for pump detection
    """
    try:
        df = pd.DataFrame(historical_prices)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.set_index('timestamp').sort_index()
        
        # Calculate key metrics
        initial_price = df['price_usd'].iloc[0]
        current_price = df['price_usd'].iloc[-1]
        peak_price = df['price_usd'].max()
        
        # Time windows to check
        time_windows = {
            'minutes': 60,          # 1 hour
            'hours': 24,            # 1 day
            'days': 7               # 1 week
        }
        
        warning_signs = []
        price_increases = {}
        volume_increases = {}
        
        for window, periods in time_windows.items():
            # Calculate price increase for each time window
            if len(df) >= periods:
                start_price = df['price_usd'].iloc[-periods]
                price_change = ((current_price - start_price) / start_price) * 100
                price_increases[window] = price_change
                
                # Calculate volume increase if volume data is available
                if 'volume_24h' in df.columns:
                    start_volume = df['volume_24h'].iloc[-periods]
                    current_volume = df['volume_24h'].iloc[-1]
                    if start_volume > 0:
                        volume_change = ((current_volume - start_volume) / start_volume) * 100
                        volume_increases[window] = volume_change

        # Determine if this is a pump based on multiple factors
        is_pump = False
        pump_score = 0
        time_frame = 'none'
        volume_increase_pct = 0
        
        # Check for extreme price increases in different time frames
        for window, increase in price_increases.items():
            if increase > 100:  # More than 100% increase
                pump_score += min(increase / 10, 50)  # Cap at 50 points
                time_frame = window
                if increase > 500:
                    warning_signs.append(f"Extreme price increase of {increase:.1f}% in {window}")
                elif increase > 200:
                    warning_signs.append(f"Significant price increase of {increase:.1f}% in {window}")

        # Check for suspicious patterns
        if initial_price <= min_price_threshold:
            pump_score += 20
            warning_signs.append(f"Very low initial price: ${initial_price:.6f}")

        # Volume analysis
        if volume_increases:
            max_volume_increase = max(volume_increases.values())
            if max_volume_increase > 500:
                pump_score += 20
                warning_signs.append(f"Suspicious volume increase: {max_volume_increase:.1f}%")
            volume_increase_pct = max_volume_increase

        # Price velocity check
        df['price_velocity'] = df['price_usd'].pct_change() / df.index.to_series().diff().dt.total_seconds()
        max_velocity = df['price_velocity'].max()
        if max_velocity > 0.01:  # More than 1% increase per second
            pump_score += 10
            warning_signs.append("Abnormal price velocity detected")

        # Determine if this is a pump
        is_pump = pump_score >= 50

        # Calculate time to peak
        time_to_peak = (df['price_usd'].idxmax() - df.index[0]).total_seconds() / 60

        # Determine risk level
        risk_level = 'low'
        if pump_score >= 80:
            risk_level = 'extreme'
        elif pump_score >= 60:
            risk_level = 'high'
        elif pump_score >= 40:
            risk_level = 'medium'

        return PumpAnalysis(
            symbol=df['symbol'].iloc[0],
            is_pump=is_pump,
            pump_score=pump_score,
            price_increase_pct=price_increases.get(time_frame, 0),
            time_frame=time_frame,
            initial_price=initial_price,
            peak_price=peak_price,
            time_to_peak=time_to_peak,
            volume_increase_pct=volume_increase_pct,
            is_suspicious=len(warning_signs) > 2,
            risk_level=risk_level,
            warning_signs=warning_signs
        )

    except Exception as e:
        print(f"Error in pump detection: {e}")
        return None

def generate_pump_alerts(analysis: PumpAnalysis) -> Dict:
    """Generate alerts based on pump analysis"""
    alerts = {
        'is_pump_alert': False,
        'alert_level': 'none',
        'messages': [],
        'warnings': [],
        'recommendations': []
    }

    try:
        if analysis.is_pump:
            alerts['is_pump_alert'] = True
            alerts['alert_level'] = analysis.risk_level

            # Add main pump alert message
            alerts['messages'].append(
                f"Potential pump detected for {analysis.symbol}! "
                f"Price increased by {analysis.price_increase_pct:.1f}% "
                f"in {analysis.time_frame}"
            )

            # Add all warning signs
            alerts['warnings'].extend(analysis.warning_signs)

            # Add recommendations based on risk level
            if analysis.risk_level in ['high', 'extreme']:
                alerts['recommendations'].extend([
                    "Extreme caution advised - this could be a pump and dump scheme",
                    "Do not FOMO into this movement",
                    "Be prepared for sudden price reversal",
                    "Consider reporting suspicious activity to exchange"
                ])
            else:
                alerts['recommendations'].extend([
                    "Monitor closely for signs of price manipulation",
                    "Use strict stop-loss orders if trading",
                    "Be cautious of social media hype"
                ])

        return alerts

    except Exception as e:
        print(f"Error generating pump alerts: {e}")
        return alerts