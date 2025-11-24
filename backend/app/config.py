from pathlib import Path
import tomllib

class Settings(dict):
    __slots__ = ("_data",)

    def __init__(self, raw: dict):
        super().__init__(raw)
        self._data = raw

    def __getattr__(self, item):
        try:
            return self._data[item]
        except KeyError:
            raise AttributeError(item)

def load_config(path: str | Path) -> Settings:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("rb") as f:
        raw = tomllib.load(f)

    return Settings(raw)
