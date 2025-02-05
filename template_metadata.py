# template_metadata.py

from typing import Dict, List
import json
import os
from datetime import datetime


class TemplateMetadata:
    def __init__(self, template_name: str):
        self.template_name = template_name
        self.metadata = {
            'name': template_name,
            'created_date': datetime.now().isoformat(),
            'last_modified': datetime.now().isoformat(),
            'version': '1.0',
            'data_sources': [],
            'calculations': [],
            'parameters': [],
            'dependencies': [],
            'usage_guidelines': '',
            'performance_notes': ''
        }

    def add_data_source(self, source: Dict) -> None:
        """Add data source information"""
        self.metadata['data_sources'].append({
            'name': source.get('name'),
            'type': source.get('type'),
            'tables': source.get('tables', []),
            'relationships': source.get('relationships', [])
        })

    def add_calculation(self, calc: Dict) -> None:
        """Add calculation information"""
        self.metadata['calculations'].append({
            'name': calc.get('name'),
            'formula': calc.get('formula'),
            'description': calc.get('description'),
            'dependencies': calc.get('dependencies', [])
        })

    def add_parameter(self, param: Dict) -> None:
        """Add parameter information"""
        self.metadata['parameters'].append({
            'name': param.get('name'),
            'type': param.get('type'),
            'default_value': param.get('default_value'),
            'allowable_values': param.get('allowable_values', [])
        })

    def set_usage_guidelines(self, guidelines: str) -> None:
        """Set usage guidelines"""
        self.metadata['usage_guidelines'] = guidelines

    def set_performance_notes(self, notes: str) -> None:
        """Set performance notes"""
        self.metadata['performance_notes'] = notes

    def save_metadata(self, output_dir: str) -> None:
        """Save metadata to JSON file"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        file_path = os.path.join(output_dir, f"{self.template_name}_metadata.json")
        with open(file_path, 'w') as f:
            json.dump(self.metadata, f, indent=4)

    def load_metadata(self, file_path: str) -> None:
        """Load metadata from JSON file"""
        with open(file_path, 'r') as f:
            self.metadata = json.load(f)