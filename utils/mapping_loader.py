import json
from pathlib import Path


class MappingLoader:
    """
    This class reads mapping JSON files (like status_mapping.json,
    region_mapping.json) and returns them as Python dictionaries,
    so the transformer can use them to map raw values.
    """

    def __init__(self):
        # This helper file lives inside the utils/ folder,
        # so mapping json files are found relative to this file
        self.base_path = Path(__file__).parent

    def load_status_mapping(self) -> dict:
        file_path = self.base_path / "status_mapping.json"
        with open(file_path, "r") as f:
            return json.load(f)

    def load_region_mapping(self) -> dict:
        file_path = self.base_path / "region_mapping.json"
        with open(file_path, "r") as f:
            return json.load(f)