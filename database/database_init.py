import pandas as pd
from sqlalchemy import create_engine
from collections.abc import Sequence
import logging


class XSLXDataLoader:
    def __init__(self):
        self._df = None

    def load_from_filename(self, filename: str, skip_rows: Sequence[int] = None):
        if skip_rows is None:
            skip_rows = [1]
        try:
            self._df = pd.read_excel(filename, engine='openpyxl', skiprows=skip_rows)
        except Exception as e:
            logging.error(f"Error loading file: {filename} due to exception: {e}")

    def get_dataframe(self):
        if self._df is None:
            logging.warning("Loaded dataframe is None, did loading from file fail?")
        return self._df


loader = XSLXDataLoader()
loader.load_from_filename('../NBA-PbP-Sample-Dataset.xlsx')

engine = create_engine('sqlite:///bball.db')
loader.get_dataframe().to_sql('play-by-play', con=engine, index=False, if_exists='replace')