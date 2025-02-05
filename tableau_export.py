# tableau_export.py
#
# *************************************************************************************
# The program will :
#
#  This program takes your existing template data (dashboards, validations, and
#  metadata) and converts it into Tableau-friendly CSV files.
#
#  It creates two main files:
#  summary files: validation_summary.csv (containing template validation results) and
#  metadata_summary.csv (containing template structure and data source information).
# *************************************************************************************

import pandas as pd
import os
import json
import logging
from tableau_connection import TableauConnector
from template_validator import TemplateValidator
from template_builder import TemplateBuilder


class TableauExporter:
    def __init__(self, db_path: str = 'sports_ecomm.db'):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.connector = TableauConnector(db_path)
        self.validator = TemplateValidator()
        self.template_builder = TemplateBuilder(db_path)

    def export_to_tableau(self, input_dir: str = "output/templates",
                          output_dir: str = "output/tableau_data"):
        """Export data in Tableau-friendly format"""
        try:
            os.makedirs(output_dir, exist_ok=True)

            # Build templates
            self.template_builder.create_sales_dashboard()
            self.template_builder.create_product_dashboard()
            self.template_builder.create_regional_dashboard()

            templates = self.template_builder.templates

            validation_data = []
            datasource_data = []
            calculation_data = []

            for template_name, template in templates.items():
                # Extract validation data
                validation = template.get('validation', {})
                validation_data.append({
                    'template_name': template_name,
                    'status': validation.get('status', 'Unknown'),
                    'timestamp': validation.get('timestamp', ''),
                    'error_count': len(validation.get('errors', [])) if validation.get('errors') else 0,
                    'warning_count': len(validation.get('warnings', [])) if validation.get('warnings') else 0
                })

                # Extract metadata
                metadata_obj = template.get('metadata')
                if metadata_obj:
                    # Extract data sources
                    for ds in getattr(metadata_obj, '_data_sources', []):
                        datasource_data.append({
                            'template_name': template_name,
                            'datasource_name': ds.get('name', ''),
                            'datasource_type': ds.get('type', ''),
                            'tables': ', '.join(ds.get('tables', []))
                        })

                    # Extract calculations
                    for calc in getattr(metadata_obj, '_calculations', []):
                        calculation_data.append({
                            'template_name': template_name,
                            'calc_name': calc.get('name', ''),
                            'formula': calc.get('formula', ''),
                            'description': calc.get('description', '')
                        })

            # Export all summaries
            summaries = {
                'validation_summary': validation_data,
                'datasource_summary': datasource_data,
                'calculation_summary': calculation_data
            }

            for name, data in summaries.items():
                if data:
                    df = pd.DataFrame(data)
                    output_path = os.path.join(output_dir, f"{name}.csv")
                    df.to_csv(output_path, index=False)
                    self.logger.info(f"Exported {name} to {output_path}")

            self.logger.info(f"Export completed to {output_dir}")

        except Exception as e:
            self.logger.error(f"Error during Tableau export: {str(e)}")
            raise


def main():
    try:
        exporter = TableauExporter()
        exporter.export_to_tableau()
        print("Export completed successfully!")
    except Exception as e:
        print(f"Export failed: {str(e)}")


if __name__ == "__main__":
    main()