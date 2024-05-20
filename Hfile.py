import os
import json

class Hfile:
    def __init__(self, file_path):
        self.file_path = f"hfiles/{file_path}.json"
        self.metadata, self.data = self.load_hfile()

    def load_hfile(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                data = json.load(file)
                return data.get('metadata', None), data.get('data', None)
        else:
            return None, None

    def save_hfile(self):
        data = {
            'data': self.data,
            'metadata': self.metadata
        }
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)
