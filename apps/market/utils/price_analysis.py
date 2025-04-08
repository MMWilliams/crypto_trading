# apps/market/utils/price_analysis.py

import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class PriceMovementAnalysis:
    symbol: str
    current_price: float
    timestamp: int
    avg_daily_change: float
    current_daily_change: float
    std_daily_change: float
    z_score: float
    is_unusual_movement: bool
    movement_type: str  # 'spike_up', 'spike_down', 'normal'
    movement_magnitude: str  # 'extreme', 'significant', 'moderate', 'normal'
    price_velocity: float  # Rate of price change
    historical_volatility: float

def analyze_price_movements(historical_prices: List[Dict], lookback_days: int = 30) -> PriceMovementAnalysis:
    """
    Analyze price movements to detect unusual changes
    """
    try:
        # Convert to DataFrame and sort
        df = pd.DataFrame(historical_prices)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.set_index('timestamp').sort_index()
        
        # Calculate daily changes
        df['daily_change_pct'] = df['price_usd'].pct_change() * 100
        
        # Get current price info
        current_price = df['price_usd'].iloc[-1]
        previous_price = df['price_usd'].iloc[-2]
        current_timestamp = int(df.index[-1].timestamp())
        
        # Calculate average daily change and standard deviation
        avg_daily_change = df['daily_change_pct'].mean()
        std_daily_change = df['daily_change_pct'].std()
        
        # Calculate current daily change
        current_daily_change = ((current_price - previous_price) / previous_price) * 100
        
        # Calculate z-score of current movement
        z_score = (current_daily_change - avg_daily_change) / std_daily_change if std_daily_change != 0 else 0
        
        # Determine if movement is unusual (beyond 2 standard deviations)
        is_unusual_movement = abs(z_score) > 2
        
        # Classify movement type
        movement_type = 'normal'
        if current_daily_change > 0:
            movement_type = 'spike_up'
        elif current_daily_change < 0:
            movement_type = 'spike_down'
            
        # Classify magnitude of movement
        movement_magnitude = 'normal'
        if abs(z_score) > 3:
            movement_magnitude = 'extreme'
        elif abs(z_score) > 2:
            movement_magnitude = 'significant'
        elif abs(z_score) > 1:
            movement_magnitude = 'moderate'
            
        # Calculate price velocity (rate of change over time)
        df['price_velocity'] = df['price_usd'].diff() / df.index.to_series().diff().dt.total_seconds()
        current_velocity = df['price_velocity'].iloc[-1]
        
        # Calculate historical volatility
        df['log_return'] = np.log(df['price_usd'] / df['price_usd'].shift(1))
        historical_volatility = df['log_return'].std() * np.sqrt(365 * 24 * 60)  # Annualized

        return PriceMovementAnalysis(
            symbol=df['symbol'].iloc[-1],
            current_price=current_price,
            timestamp=current_timestamp,
            avg_daily_change=avg_daily_change,
            current_daily_change=current_daily_change,
            std_daily_change=std_daily_change,
            z_score=z_score,
            is_unusual_movement=is_unusual_movement,
            movement_type=movement_type,
            movement_magnitude=movement_magnitude,
            price_velocity=current_velocity,
            historical_volatility=historical_volatility
        )
        
    except Exception as e:
        print(f"Error in price movement analysis: {e}")
        return None

def generate_price_alerts(analysis: PriceMovementAnalysis) -> Dict:
    """
    Generate alerts based on price movement analysis
    """
    alerts = {
        'has_alerts': False,
        'alert_level': 'none',  # none, low, medium, high
        'messages': [],
        'recommendations': []
    }
    
    try:
        if analysis.is_unusual_movement:
            alerts['has_alerts'] = True
            
            # Determine alert level
            if analysis.movement_magnitude == 'extreme':
                alerts['alert_level'] = 'high'
            elif analysis.movement_magnitude == 'significant':
                alerts['alert_level'] = 'medium'
            elif analysis.movement_magnitude == 'moderate':
                alerts['alert_level'] = 'low'
                
            # Generate alert messages
            if analysis.movement_type == 'spike_up':
                alerts['messages'].append(
                    f"Unusual upward price movement detected: {analysis.current_daily_change:.2f}% "
                    f"change (typical change: {analysis.avg_daily_change:.2f}%)"
                )
                alerts['recommendations'].append(
                    "Consider taking profits or setting stop-loss orders"
                )
            else:
                alerts['messages'].append(
                    f"Unusual downward price movement detected: {analysis.current_daily_change:.2f}% "
                    f"change (typical change: {analysis.avg_daily_change:.2f}%)"
                )
                alerts['recommendations'].append(
                    "Monitor for potential buying opportunity if downward trend stabilizes"
                )
                
            # Add volatility context
            if analysis.historical_volatility > 1:
                alerts['messages'].append(
                    f"High historical volatility detected: {analysis.historical_volatility:.2f}"
                )
                alerts['recommendations'].append(
                    "Exercise caution due to high market volatility"
                )
                
        return alerts
        
    except Exception as e:
        print(f"Error generating price alerts: {e}")
        return alerts