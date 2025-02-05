# tableau_export.py

from tableau_connection import TableauConnector
from calculation_library import CalculationLibrary
from template_metadata import TemplateMetadata
from template_validator import TemplateValidator
import logging
import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import datetime


class TableauExporter:
    def __init__(self, db_path: str = 'sports_ecomm.db', template_dir: str = 'output/templates'):
        self.connector = TableauConnector(db_path)
        self.calc_library = CalculationLibrary()
        self.validator = TemplateValidator()
        self.template_dir = template_dir
        self.templates = {}
        self.data_sources = None

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.load_templates()

    def load_templates(self):
        """Load existing templates from template directory"""
        try:
            template_types = ['sales_dashboard', 'product_dashboard', 'regional_dashboard']
            for template_type in template_types:
                metadata_path = os.path.join(self.template_dir, f"{template_type}_metadata.json")
                validation_path = os.path.join(self.template_dir, f"{template_type}_validation.json")

                try:
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                    with open(validation_path, 'r') as f:
                        validation = json.load(f)

                    self.templates[template_type] = {
                        'metadata': metadata,
                        'validation': validation
                    }
                except FileNotFoundError:
                    self.templates[template_type] = {
                        'metadata': {
                            'name': template_type,
                            'fields': [],
                            'calculations': [],
                            'data_sources': []
                        },
                        'validation': {}
                    }

            self.logger.info("Templates loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load templates: {str(e)}")
            raise

    def prepare_dashboard_data(self, dashboard_type: str) -> pd.DataFrame:
        """Prepare data according to template specifications"""
        try:
            data_sources = self.connector.get_data_source()

            if dashboard_type == 'sales_dashboard':
                # Join relevant tables
                sales_df = data_sources['fact_daily_sales']
                dates_df = data_sources['dimension_dates']
                products_df = data_sources['dimension_products']

                # Convert full_date to datetime before merging
                dates_df['full_date'] = pd.to_datetime(dates_df['full_date'])

                df = (sales_df.merge(dates_df, on='date_id')
                      .merge(products_df, on='product_id'))

                # Pre-compute all calculations
                df['Product_Revenue'] = df['quantity_sold'] * df['unit_price']
                df['Growth_Rate'] = df.groupby('product_id')['total_price'].pct_change()
                df['YTD_Sales'] = df.groupby(['product_id', df['full_date'].dt.year])['total_price'].cumsum()
                df['Sales_Target_Achievement'] = df['total_price'] / df.groupby('product_id')['total_price'].transform(
                    'mean')
                df['Discount_Impact'] = (df['discount_applied'] * df['quantity_sold'])

            elif dashboard_type == 'product_dashboard':
                products_df = data_sources['dimension_products']
                sales_df = data_sources['fact_daily_sales']

                df = products_df.merge(sales_df, on='product_id')

                # Pre-compute all calculations
                df['Product_Revenue'] = df['quantity_sold'] * df['unit_price']
                df['Product_Profit'] = df['total_price'] - (df['quantity_sold'] * df['base_price'])
                df['Product_Profit_Margin'] = df['Product_Profit'] / df['total_price']
                df['Stock_Turnover'] = df.groupby('product_id')['quantity_sold'].transform('sum')
                df['Avg_Unit_Price'] = df.groupby('product_id')['unit_price'].transform('mean')
                df['Total_Revenue'] = df.groupby('product_id')['total_price'].transform('sum')

            elif dashboard_type == 'regional_dashboard':
                sales_df = data_sources['fact_daily_sales']
                dates_df = data_sources['dimension_dates']

                # Convert full_date to datetime before merging
                dates_df['full_date'] = pd.to_datetime(dates_df['full_date'])

                df = sales_df.merge(dates_df, on='date_id')

                # Pre-compute all calculations using available columns
                total_revenue = df['total_price'].sum()
                df['Regional_Revenue'] = df.groupby('region')['total_price'].transform('sum')
                df['Regional_Market_Share'] = df['Regional_Revenue'] / total_revenue
                df['Regional_Total_Units'] = df.groupby('region')['quantity_sold'].transform('sum')
                df['Regional_Avg_Price'] = df.groupby('region')['unit_price'].transform('mean')
                df['Regional_Total_Discount'] = df.groupby('region')['discount_applied'].transform('sum')

                # Add channel performance metrics
                df['Channel_Revenue'] = df.groupby(['region', 'channel'])['total_price'].transform('sum')
                df['Channel_Share'] = df['Channel_Revenue'] / df['Regional_Revenue']

            # Add timestamp and refresh info
            df['data_refresh_date'] = datetime.now().strftime('%Y-%m-%d')
            df['dashboard_type'] = dashboard_type

            # Convert data types to Tableau-friendly formats
            for column in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    df[column] = df[column].dt.strftime('%Y-%m-%d')
                elif pd.api.types.is_numeric_dtype(df[column]):
                    df[column] = df[column].astype(float)
                else:
                    df[column] = df[column].astype(str)

            return df

        except Exception as e:
            self.logger.error(f"Failed to prepare data for {dashboard_type}: {str(e)}")
            raise

    def export_dashboard(self, dashboard_type: str, output_dir: str) -> None:
        """Export single dashboard in Tableau-friendly format"""
        try:
            if dashboard_type not in self.templates:
                raise ValueError(f"No template found for {dashboard_type}")

            # Prepare dashboard data
            df = self.prepare_dashboard_data(dashboard_type)

            # Create flat JSON structure
            dashboard_data = {
                "tableauData": df.to_dict(orient='records')
            }

            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            # Save dashboard file
            dashboard_path = os.path.join(output_dir, f"{dashboard_type}.json")
            with open(dashboard_path, 'w') as f:
                json.dump(dashboard_data, f, indent=4, default=str)

            self.logger.info(f"Dashboard {dashboard_type} exported successfully")
            self.verify_export(dashboard_path)

        except Exception as e:
            self.logger.error(f"Failed to export {dashboard_type}: {str(e)}")
            raise

    def export_all(self, output_dir: str = 'output/tableau_data') -> None:
        """Export all dashboards in a combined format"""
        try:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            all_records = []

            # Process each dashboard type
            for dashboard_type in ['sales_dashboard', 'product_dashboard', 'regional_dashboard']:
                # Get data with pre-computed calculations
                df = self.prepare_dashboard_data(dashboard_type)

                # Export individual dashboard
                dashboard_data = {
                    "tableauData": df.to_dict(orient='records')
                }

                # Save individual dashboard file
                with open(os.path.join(output_dir, f'{dashboard_type}.json'), 'w') as f:
                    json.dump(dashboard_data, f, indent=4, default=str)

                # Add to combined records
                all_records.extend(df.to_dict(orient='records'))

            # Save combined file with flat structure
            combined_data = {
                "tableauData": all_records
            }

            combined_path = os.path.join(output_dir, 'tableau_dashboards.json')
            with open(combined_path, 'w') as f:
                json.dump(combined_data, f, indent=4, default=str)

            self.logger.info(f"All dashboards exported to {output_dir}")
            self._print_connection_instructions(output_dir)

        except Exception as e:
            self.logger.error(f"Failed to export dashboards: {str(e)}")
            raise

    def verify_export(self, file_path: str) -> None:
        """Verify the exported JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            print(f"\nVerifying {os.path.basename(file_path)}:")
            if 'tableauData' in data:  # Changed from 'data' to 'tableauData'
                records = data['tableauData']  # Changed from data['data']
                print(f"Number of records: {len(records)}")
                if records:
                    print("\nFields available:")
                    for field in records[0].keys():
                        print(f"- {field}")
                    print("\nSample record:")
                    print(json.dumps(records[0], indent=2))
            else:
                print("Warning: No 'tableauData' key found in JSON")  # Updated warning message

        except Exception as e:
            print(f"Verification error: {str(e)}")

    def _print_connection_instructions(self, output_dir: str) -> None:
        """Print connection instructions for all exported files"""
        print("\nExport completed! Here's how to connect to each dashboard:\n")

        dashboard_types = ['sales_dashboard', 'product_dashboard', 'regional_dashboard']
        for dashboard_type in dashboard_types:
            print(f"For {dashboard_type.replace('_', ' ').title()}:")
            print(f"1. Connect to: {os.path.join(output_dir, f'{dashboard_type}.json')}")
            print("2. Select 'tableauData' in schema")  # Changed from 'data'
            print("3. All calculations are pre-computed in the data\n")  # Updated instruction

        print("For Combined Dashboard:")
        print(f"1. Connect to: {os.path.join(output_dir, 'tableau_dashboards.json')}")
        print("2. Select 'tableauData' in schema")  # Changed from 'data'
        print("3. Use 'dashboard_type' field to filter different views")
        print("4. All calculations are pre-computed in the data")  # Updated instruction


def main():
    try:
        # Initialize exporter
        exporter = TableauExporter()

        # Create output directory
        output_dir = 'output/tableau_data'
        os.makedirs(output_dir, exist_ok=True)

        # Export all dashboards
        exporter.export_all(output_dir)

    except Exception as e:
        print(f"Export failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()