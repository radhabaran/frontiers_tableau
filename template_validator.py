# template_validator.py

from typing import Dict, List
import pandas as pd
import time
import numpy as np


class TemplateValidator:
    def __init__(self):
        self.validation_results = {
            'data_source': [],
            'calculations': [],
            'performance': [],
            'errors': []
        }

    def _convert_to_serializable(self, obj):
        """Convert pandas/numpy types to JSON serializable types"""
        if isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, pd.Series):
            return obj.to_list()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, 'dtype'):
            return str(obj.dtype)
        return obj

    def validate_data_source(self, data_source: Dict[str, pd.DataFrame]) -> bool:
        """Validate data source integrity"""
        try:
            for table_name, df in data_source.items():
                # Check for null values
                null_check = df.isnull().sum()

                # Check for data types
                dtype_check = df.dtypes

                # Check for duplicate records
                duplicate_check = df.duplicated().sum()

                self.validation_results['data_source'].append({
                    'table': table_name,
                    'row_count': int(len(df)),
                    'null_values': {k: int(v) for k, v in null_check.to_dict().items()},
                    'data_types': {k: str(v) for k, v in dtype_check.to_dict().items()},
                    'duplicates': int(duplicate_check)
                })

            return True

        except Exception as e:
            self.validation_results['errors'].append(str(e))
            return False

    def test_calculations(self, calc_library) -> bool:
        """Test calculation accuracy"""
        try:
            # Test sample calculations
            test_data = pd.Series([100, 120, 150, 140, 160])

            tests = {
                'yoy_growth': float(calc_library.year_over_year_growth(test_data) or 0),
                'moving_avg': [float(x) for x in calc_library.moving_average(test_data).fillna(0).tolist()],
                'sales_velocity': float(calc_library.sales_velocity(100, 7))
            }

            self.validation_results['calculations'] = tests
            return True

        except Exception as e:
            self.validation_results['errors'].append(str(e))
            return False

    def test_performance(self, template_path: str) -> bool:
        """Test template performance"""
        try:
            start_time = time.time()
            # Add performance testing logic here
            end_time = time.time()

            self.validation_results['performance'].append({
                'template': template_path,
                'load_time': float(end_time - start_time),
                'memory_usage': 'TBD'  # Add memory profiling
            })

            return True

        except Exception as e:
            self.validation_results['errors'].append(str(e))
            return False

    def get_validation_report(self) -> Dict:
        """Generate validation report"""
        # Convert all values to JSON serializable format
        serializable_results = {}
        for key, value in self.validation_results.items():
            if isinstance(value, list):
                serializable_results[key] = [
                    {k: self._convert_to_serializable(v) for k, v in item.items()}
                    if isinstance(item, dict) else self._convert_to_serializable(item)
                    for item in value
                ]
            else:
                serializable_results[key] = self._convert_to_serializable(value)

        return {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'results': serializable_results,
            'status': 'PASS' if not self.validation_results['errors'] else 'FAIL'
        }