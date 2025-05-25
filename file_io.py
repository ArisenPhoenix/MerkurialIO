from MerkurialIO import Path, json, gzip, BeautifulSoup, pickle, csv, IO, TextIO, BinaryIO, cast, Iterable, Set, Collection
from MerkurialGlobals import PathType, SupportsWrite

class FileIOMixin:
    def __init__(self, source_dir: PathType = None, **kwargs):
        self.SOURCE_DIR = Path(source_dir) if source_dir else None

        super().__init__(**kwargs)

    # ------- I/O HELPERS -------
    def open_text(self, path: PathType, mode: str = "rt", compress: bool = False) -> TextIO:
        return gzip.open(path, mode, encoding="utf-8") if compress or path.endswith(".gz") else open(path, mode, encoding="utf-8")

    def open_binary(self, path: PathType, mode: str = "rb", compress: bool = False) -> BinaryIO | IO:
        return gzip.open(path, mode) if compress or path.endswith(".gz") else open(path, mode)

    # ------- INGESTION -------
    def ingest_category_files(self, category: str) -> Set[str]:
        if not self.SOURCE_DIR:
            raise ValueError("source_dir not set for ingestion.")
        result = set()
        for ext, reader in [("*.txt", self._read_txt), ("*.html", self._read_html)]:
            for file in self.SOURCE_DIR.glob(ext):
                if category.lower() in file.name.lower():
                    result.update(reader(file))
        return result

    def _read_txt(self, path: PathType) -> Set[str]:
        try:
            with open(path, encoding="utf-8") as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            return set()

    def _read_html(self, path: PathType) -> Set[str]:
        try:
            with open(path, encoding="utf-8") as f:
                soup = BeautifulSoup(f, "html.parser")
                return {a["href"].strip() for a in soup.find_all("a", href=True)}
        except Exception as e:
            print(f"[!] Failed to read html {path}: {e}")
            return set()

    # ------- IMPORT / EXPORT -------
    def import_json(self, path: PathType, keys: Iterable[str], compress: bool = False):
        with self.open_text(path, "rt", compress) as f:
            data = json.load(f)
        for k in keys:
            if not self.state.get(k):
                self.state[k] = set(data.get(k, []))
            else:
                self.state[k].update(data.get(k, []))

    def import_pickle(self, path: PathType, keys: Iterable[str] = None, compress: bool = False):
        with self.open_binary(path, "rb", compress) as f:
            state = pickle.load(f)
        for k in (keys or self.keys):
            self.state[k] = set(state.get(k, []))

    def import_csv(self, path: PathType, keys: Iterable[str] = None, compress: bool = False):
        with self.open_text(path, "rt", compress) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            for key in headers:
                if key not in (keys or self.keys):
                    raise ValueError(f"Invalid header '{key}' found in CSV. Must be one of: {keys}")

            for row in reader:
                for key, value in row.items():
                    if value and key in (keys or self.keys):
                        self.state[key].add(value.strip())

    def export_json(self, path: PathType, compress: bool = False):
        with self.open_text(path, "wt", compress) as f:
            json.dump(self.state.to_json(), cast(SupportsWrite, f), indent=4)

    def export_csv(self, path: PathType, keys: Iterable[str] | Collection[str], compress: bool = False):
        max_len = max(len(self.state[k]) for k in keys)
        rows = []

        for i in range(max_len):
            row = {}
            for k in keys:
                vals = list(self.state[k])
                row[k] = vals[i] if i < len(vals) else ""
            rows.append(row)

        with self.open_text(path, "wt", compress) as f:
            writer = csv.DictWriter(cast(SupportsWrite, f), fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)

    def export_pickle(self, path: PathType, compress: bool = False):
        with self.open_binary(path, "wb", compress) as f:
            pickle.dump(self.state.to_json(), cast(SupportsWrite, f))