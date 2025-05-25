from MerkurialIO import Path, FileLock, json, os, Iterable
from MerkurialGlobals import PathType, INDENT, META_FILE_NAME  # A Dependency meant to allow for flexibility and configuration across libraries


class PersistenceMixin:
    def __init__(self, state_dir: PathType, **kwargs):
        super().__init__(**kwargs)
        self._state_dir = Path(state_dir)
        self._meta_file_path = self._state_dir / f"{META_FILE_NAME}"

    @property
    def META_FILE_PATH(self) -> Path:
        return self._meta_file_path

    @META_FILE_PATH.setter
    def META_FILE_PATH(self, path: PathType) -> None:
        self._meta_file_path = Path(path)

    @property
    def STATE_DIR(self):
        return self._state_dir

    @STATE_DIR.setter
    def STATE_DIR(self, path: PathType):
        self._state_dir = Path(path)

    def lock_path(self, path: PathType = None) -> Path:
        path = Path(path) if path is not None else self.META_FILE_PATH
        return Path(str(path)+".lock")

    def create_dir(self):
        self.STATE_DIR.mkdir(parents=True, exist_ok=True)

    def _decide_path(self, path: PathType = None):
        return Path(path) if path is not None else self.META_FILE_PATH


    def save(self, path: PathType = None):
        path = self._decide_path(path)
        self.META_FILE_PATH = path
        with FileLock(self.lock_path(path)):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._state.to_json(), f, indent=INDENT)

    def read_data(self, path: PathType = None):
        path = self._decide_path(path)
        if not path.exists():
            return False
        with FileLock(self.lock_path(path)):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    def data_exists(self, path: PathType = None):
        path = self._decide_path(path)
        with FileLock(self.lock_path(path)):
            return path.exists() and os.path.getsize(path) > 0

    def create_json(self, data, path: PathType = None):
        path = self._decide_path(path)
        if not os.path.exists(path):
            with FileLock(self.lock_path(path)):
                with open(path, "w", encoding= "utf-8") as f:
                    json.dump(data, f)


    def load(self, schema, key = None):
        path = self.META_FILE_PATH
        if not path.exists():
            return False

        with FileLock(self.lock_path()):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                try:
                    if isinstance(key, str):
                        self._state = self.DataWrapper(data, schema.get(key), fill_defaults=True)
                    else:
                        self._state = self.DataWrapper(data, schema, fill_defaults=True)
                except Exception as e:
                    raise Exception(e)

        return True


