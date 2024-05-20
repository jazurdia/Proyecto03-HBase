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
                return data.get('metadata', None), {key: data[key] for key in data if key != 'metadata'}
        else:
            return None, None

    def save_hfile(self):
        with open(self.file_path, 'w') as file:
            data = {'metadata': self.metadata, 'data': self.data}
            json.dump(data, file, indent=4)

    
        