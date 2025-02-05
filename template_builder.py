# template_builder.py

from tableau_connection import TableauConnector
from calculation_library import CalculationLibrary
from template_metadata import TemplateMetadata
from template_validator import TemplateValidator
import logging
import os
import json
from typing import Dict, List


class TemplateBuilder:
    def __init__(self, db_path: str = 'sports_ecomm.db'):
        self.connector = TableauConnector(db_path)
        self.calc_library = CalculationLibrary()
        self.validator = TemplateValidator()
        self.templates = {}

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def create_sales_dashboard(self) -> None:
        """Create sales performance dashboard template"""
        try:
            # Get data
            data_sources = self.connector.get_data_source()

            # Create metadata
            metadata = TemplateMetadata('sales_dashboard')
            metadata.add_data_source({
                'name': 'sales_data',
                'type': 'extract',
                'tables': list(data_sources.keys())
            })

            # Add calculations
            calcs = self.calc_library.create_calculated_fields()
            for name, formula in calcs.items():
                metadata.add_calculation({
                    'name': name,
                    'formula': formula,
                    'description': f'Standard {name} calculation'
                })

            # Validate template
            self.validator.validate_data_source(data_sources)
            self.validator.test_calculations(self.calc_library)

            # Store template information
            self.templates['sales_dashboard'] = {
                'metadata': metadata,
                'validation': self.validator.get_validation_report()
            }

            self.logger.info("Sales dashboard template created successfully")

        except Exception as e:
            self.logger.error(f"Failed to create sales dashboard: {str(e)}")

    def create_product_dashboard(self) -> None:
        """Create product analytics dashboard template"""
        # Similar implementation for product dashboard
        pass

    def create_regional_dashboard(self) -> None:
        """Create regional performance dashboard template"""
        # Similar implementation for regional dashboard
        pass

    def export_templates(self, output_dir: str) -> None:
        """Export all templates and metadata"""
        try:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            for template_name, template_data in self.templates.items():
                # Save metadata
                template_data['metadata'].save_metadata(output_dir)

                # Save validation report
                report_path = os.path.join(output_dir, f"{template_name}_validation.json")
                validation_data = template_data['validation']

                # Ensure all data is JSON serializable
                with open(report_path, 'w') as f:
                    json.dump(validation_data, f, indent=4, default=str)

            self.logger.info(f"Templates exported to {output_dir}")

        except Exception as e:
            self.logger.error(f"Failed to export templates: {str(e)}")


def main():
    # Initialize builder
    builder = TemplateBuilder()

    # Create templates
    builder.create_sales_dashboard()
    builder.create_product_dashboard()
    builder.create_regional_dashboard()

    # Export templates
    builder.export_templates('output/templates')


if __name__ == "__main__":
    main()