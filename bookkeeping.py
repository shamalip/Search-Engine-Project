import json

base_path = "WEBPAGES_RAW"

files_map = {}
with open(base_path + "/bookkeeping.json") as f:
    files_map = json.loads(f.read())
inv_map = {v: k for k, v in files_map.items()}