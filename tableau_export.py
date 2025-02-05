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
                        'metadata': {
                            'name': template_type,
                            'fields': metadata.get('fields', []),
                            'calculations': metadata.get('calculations', {}),
                            'data_sources': metadata.get('data_sources', [])
                        },
                        'validation': validation
                    }
                except FileNotFoundError:
                    self.templates[template_type] = {
                        'metadata': {
                            'name': template_type,
                            'fields': [],
                            'calculations': {},
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
            if not self.data_sources:
                self.data_sources = self.connector.get_data_source()

            # Create sample data if real data is not available
            if dashboard_type == 'sales_dashboard':
                df = pd.DataFrame({
                    'date': pd.date_range(start='2024-01-01', periods=100),
                    'product_name': [f'Product {i}' for i in range(1, 101)],
                    'sales_amount': np.random.randint(100, 1000, 100),
                    'quantity_sold': np.random.randint(1, 50, 100),
                    'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
                    'customer_id': np.random.randint(1000, 9999, 100)
                })

                # Add calculated columns
                df['total_revenue'] = df['sales_amount'] * df['quantity_sold']
                df['avg_price'] = df['sales_amount'] / df['quantity_sold']

            elif dashboard_type == 'product_dashboard':
                df = pd.DataFrame({
                    'product_id': range(1, 51),
                    'product_name': [f'Product {i}' for i in range(1, 51)],
                    'category': np.random.choice(['Sports', 'Fitness', 'Outdoor', 'Accessories'], 50),
                    'unit_price': np.random.uniform(10, 200, 50),
                    'stock_level': np.random.randint(0, 1000, 50),
                    'reorder_point': np.random.randint(10, 100, 50)
                })

                # Add product metrics
                df['stock_status'] = np.where(df['stock_level'] > df['reorder_point'],
                                              'Adequate', 'Low')

            elif dashboard_type == 'regional_dashboard':
                df = pd.DataFrame({
                    'region': np.repeat(['North', 'South', 'East', 'West'], 25),
                    'channel': np.random.choice(['Online', 'Store', 'Distributor'], 100),
                    'total_sales': np.random.randint(10000, 50000, 100),
                    'customer_count': np.random.randint(100, 500, 100),
                    'date': pd.date_range(start='2024-01-01', periods=100)
                })

                # Add regional metrics
                df['avg_customer_value'] = df['total_sales'] / df['customer_count']

            # Convert data types to Tableau-friendly formats
            for column in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
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

            # Create Tableau-friendly JSON structure
            dashboard_data = {
                "data": df.to_dict(orient='records')
            }

            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            # Save dashboard file
            dashboard_path = os.path.join(output_dir, f"{dashboard_type}.json")
            with open(dashboard_path, 'w') as f:
                json.dump(dashboard_data, f, indent=4, default=str)

            self.logger.info(f"Dashboard {dashboard_type} exported successfully")

            # Verify the export
            self.verify_export(dashboard_path)

        except Exception as e:
            self.logger.error(f"Failed to export {dashboard_type}: {str(e)}")
            raise

    def export_all(self, output_dir: str = 'output/tableau_data') -> None:
        """Export all dashboards in a combined format"""
        try:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Export individual dashboards and store their data separately
            sales_data = self.prepare_dashboard_data('sales_dashboard')
            product_data = self.prepare_dashboard_data('product_dashboard')
            regional_data = self.prepare_dashboard_data('regional_dashboard')

            # Export individual dashboard files
            for dashboard_type, data in {
                'sales_dashboard': sales_data,
                'product_dashboard': product_data,
                'regional_dashboard': regional_data
            }.items():
                dashboard_path = os.path.join(output_dir, f"{dashboard_type}.json")
                with open(dashboard_path, 'w') as f:
                    json.dump({"data": data.to_dict(orient='records')}, f, indent=4, default=str)

            # Create separate JSONs for each dashboard type
            sales_json = {
                "data": {
                    "sales": sales_data.to_dict(orient='records')
                }
            }

            product_json = {
                "data": {
                    "products": product_data.to_dict(orient='records')
                }
            }

            regional_json = {
                "data": {
                    "regional": regional_data.to_dict(orient='records')
                }
            }

            # Save individual type files
            with open(os.path.join(output_dir, 'sales_data.json'), 'w') as f:
                json.dump(sales_json, f, indent=4, default=str)

            with open(os.path.join(output_dir, 'product_data.json'), 'w') as f:
                json.dump(product_json, f, indent=4, default=str)

            with open(os.path.join(output_dir, 'regional_data.json'), 'w') as f:
                json.dump(regional_json, f, indent=4, default=str)

            # Combined structured format
            combined_data = {
                "data": {
                    "sales": sales_data.to_dict(orient='records'),
                    "products": product_data.to_dict(orient='records'),
                    "regional": regional_data.to_dict(orient='records')
                }
            }

            # Save combined file
            combined_path = os.path.join(output_dir, 'tableau_dashboards.json')
            with open(combined_path, 'w') as f:
                json.dump(combined_data, f, indent=4, default=str)

            self.logger.info(f"All dashboards exported to {output_dir}")

            # Verify all exports
            print("\nVerifying individual exports:")
            self.verify_export(os.path.join(output_dir, 'sales_data.json'))
            self.verify_export(os.path.join(output_dir, 'product_data.json'))
            self.verify_export(os.path.join(output_dir, 'regional_data.json'))
            print("\nVerifying combined export:")
            self.verify_export(combined_path)

        except Exception as e:
            self.logger.error(f"Failed to export dashboards: {str(e)}")
            raise

    def verify_export(self, file_path: str) -> None:
        """Verify the exported JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            print(f"\nVerifying {os.path.basename(file_path)}:")
            if 'data' in data:
                if isinstance(data['data'], dict):
                    for key, records in data['data'].items():
                        print(f"\n{key.upper()} Dashboard:")
                        print(f"Number of records: {len(records)}")
                        if records:
                            print("Fields available:")
                            for field in records[0].keys():
                                print(f"- {field}")
                            print("\nSample record:")
                            print(json.dumps(records[0], indent=2))
                else:
                    records = data['data']
                    print(f"Number of records: {len(records)}")
                    if records:
                        print("Fields available:")
                        for field in records[0].keys():
                            print(f"- {field}")
                        print("\nSample record:")
                        print(json.dumps(records[0], indent=2))
            else:
                print("Warning: No 'data' key found in JSON")

        except Exception as e:
            print(f"Verification error: {str(e)}")


def main():
    try:
        # Initialize exporter
        exporter = TableauExporter()

        # Create output directory
        output_dir = 'output/tableau_data'
        os.makedirs(output_dir, exist_ok=True)

        # Export all dashboards
        exporter.export_all(output_dir)

        # Print connection instructions
        print("\nExport completed successfully!")
        print("\nTo connect in Tableau:")
        print("1. Open Tableau Public")
        print("2. Connect to JSON file")
        print("3. You can connect to any of these files:")
        print(f"   - Combined dashboard: {os.path.join(output_dir, 'tableau_dashboards.json')}")
        print(f"   - Sales dashboard: {os.path.join(output_dir, 'sales_data.json')}")
        print(f"   - Product dashboard: {os.path.join(output_dir, 'product_data.json')}")
        print(f"   - Regional dashboard: {os.path.join(output_dir, 'regional_data.json')}")
        print("4. In the schema section, select 'data' and then the dashboard type")
        print("5. Click 'Update Now'")

    except Exception as e:
        print(f"Export failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()