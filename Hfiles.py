import os
import json
from collections import defaultdict
import time

class HFile:
    def __init__(self):
        self.data = defaultdict(lambda: defaultdict(dict))
        self.enabled = True

    def put(self, row_key, column_family, column_qualifier, value):
        timestamp = int(time.time())
        self.data[row_key][column_family][column_qualifier] = (value, timestamp)

    def get(self, row_key, column_family, column_qualifier):
        if row_key in self.data and column_family in self.data[row_key] and column_qualifier in self.data[row_key][column_family]:
            return self.data[row_key][column_family][column_qualifier]
        return None

    def scan(self):
        result = []
        for row_key in sorted(self.data.keys()):
            for cf, qualifiers in self.data[row_key].items():
                for cq, (value, ts) in qualifiers.items():
                    result.append((row_key, cf, cq, ts, value))
        return result

    def delete(self, row_key, column_family, column_qualifier):
        if row_key in self.data and column_family in self.data[row_key]:
            if column_qualifier in self.data[row_key][column_family]:
                del self.data[row_key][column_family][column_qualifier]

    def delete_all(self, row_key):
        if row_key in self.data:
            del self.data[row_key]

class Table:
    def __init__(self, name):
        self.name = name
        self.hfile = HFile()

    def is_enabled(self):
        return self.hfile.enabled

    def disable(self):
        self.hfile.enabled = False

    def enable(self):
        self.hfile.enabled = True

class HBaseSimulator:
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name)
            print(f"=> Hbase::Table - '{table_name}'")
        else:
            print(f"=> Hbase::Table '{table_name}' already exists.")

    def drop_table(self, table_name):
        if table_name in self.tables:
            del self.tables[table_name]
            print(f"=> Hbase::Table '{table_name}' dropped.")
        else:
            print(f"=> Hbase::Table '{table_name}' does not exist.")

    def list_tables(self):
        return list(self.tables.keys())
