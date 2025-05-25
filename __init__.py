__version__ = "0.0.1"
__author__ = "Brandon Marcure"
__credits__ = "Brandon Marcure @ brandon@merkurialphoenix.com"


# --- General Utilities ---
import os
import json
import gzip
import pickle
import csv

# --- Filesystem ---
from pathlib import Path
from filelock import FileLock

# --- Typing ---
from typing import IO, TextIO, BinaryIO, Iterable, Set, Collection, List, Dict, cast

# --- External Parser ---
from bs4 import BeautifulSoup

# --- Global Constants ---
from MerkurialGlobals import PathType, INDENT, SupportsWrite

# --- Internal Classes ---
from .file_io import FileIOMixin
from .persistence_io import PersistenceMixin

__all__ = [
    "os", "json", "gzip", "pickle", "csv",
    "Path", "FileLock",
    "IO", "TextIO", "BinaryIO", "Iterable", "Set", "Collection", "List", "Dict", "cast",
    "BeautifulSoup",
    "FileIOMixin", "PersistenceMixin"
]