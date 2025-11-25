import json
import os

class ModelLoader:
    def __init__(self, path):
        self.path = path
        self.data = None

    def load(self):
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"El archivo {self.path} no existe")

        with open(self.path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

        return self.data
