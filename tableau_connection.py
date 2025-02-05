# tableau_connection.py

import sqlite3
import pandas as pd
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, TableDefinition, SqlType, Inserter
from typing import Dict, List


class TableauConnector:
    def __init__(self, db_path: str = 'sports_ecomm.db'):
        self.db_path = db_path
        self.tables = [
            'fact_daily_sales',
            'dimension_products',
            'dimension_dates'
        ]

    def get_data_source(self) -> Dict[str, pd.DataFrame]:
        """Extract data from SQLite and prepare for Tableau"""
        try:
            conn = sqlite3.connect(self.db_path)
            data_frames = {}

            for table in self.tables:
                query = f"SELECT * FROM {table}"
                data_frames[table] = pd.read_sql_query(query, conn)

            conn.close()
            return data_frames

        except Exception as e:
            raise Exception(f"Data source extraction failed: {str(e)}")

    def create_extract(self, output_path: str) -> bool:
        """Create Tableau Hyper Extract"""
        try:
            with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(hyper.endpoint, output_path, CreateMode.CREATE_AND_REPLACE) as connection:
                    data_frames = self.get_data_source()

                    for table_name, df in data_frames.items():
                        # Create table definition
                        table_def = self._create_table_definition(table_name, df)
                        connection.catalog.create_table(table_def)

                        # Insert data
                        with Inserter(connection, table_def) as inserter:
                            inserter.add_rows(df.values.tolist())

                    return True

        except Exception as e:
            raise Exception(f"Extract creation failed: {str(e)}")

    def _create_table_definition(self, table_name: str, df: pd.DataFrame) -> TableDefinition:
        """Create Tableau table definition based on DataFrame schema"""
        columns = []
        for col_name, dtype in df.dtypes.items():
            if dtype == 'int64':
                sql_type = SqlType.int()
            elif dtype == 'float64':
                sql_type = SqlType.double()
            else:
                sql_type = SqlType.text()
            columns.append(TableDefinition.Column(col_name, sql_type))

        return TableDefinition(table_name, columns)

    def schedule_refresh(self, interval: str = 'daily') -> None:
        """Setup extract refresh schedule"""
        # Implementation for scheduling refresh
        pass