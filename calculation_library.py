# calculation_library.py

from typing import Dict, List
import pandas as pd
import numpy as np

class CalculationLibrary:
    @staticmethod
    def year_over_year_growth(data: pd.Series) -> float:
        """Calculate YoY growth"""
        current = data.iloc[-1]
        previous = data.iloc[-13] if len(data) >= 13 else None
        if previous:
            return ((current - previous) / previous) * 100
        return None

    @staticmethod
    def moving_average(data: pd.Series, window: int = 7) -> pd.Series:
        """Calculate moving average"""
        return data.rolling(window=window).mean()

    @staticmethod
    def sales_velocity(quantity: float, time_period: int) -> float:
        """Calculate sales velocity"""
        return quantity / time_period if time_period != 0 else 0

    @staticmethod
    def conversion_rate(transactions: int, visits: int) -> float:
        """Calculate conversion rate"""
        return (transactions / visits * 100) if visits != 0 else 0

    @staticmethod
    def profit_margin(revenue: float, cost: float) -> float:
        """Calculate profit margin"""
        return ((revenue - cost) / revenue * 100) if revenue != 0 else 0

    def create_calculated_fields(self) -> Dict[str, str]:
        """Return Tableau calculated field definitions"""
        return {
            'YoY Growth': 'FIXED [Year]: (SUM([Sales]) - LOOKUP(SUM([Sales]), -1)) / LOOKUP(SUM([Sales]), -1)',
            'Moving Avg': 'WINDOW_AVG(SUM([Sales]), -6, 0)',
            'Sales Velocity': 'SUM([Quantity]) / DATEDIFF("day", MIN([Date]), MAX([Date]))',
            'Conversion Rate': 'SUM([Transactions]) / SUM([Visits]) * 100',
            'Profit Margin': '(SUM([Revenue]) - SUM([Cost])) / SUM([Revenue]) * 100'
        }