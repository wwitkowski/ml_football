from datetime import datetime

import pandas as pd
from pathlib import Path


START_DATE = datetime(2000, 1, 1).date()
LEAGUES = (
    'E0', 'E1', 'E2', 'E3', 'EC', 'SC0', 'SC1', 'SC2', 'SC3', 'D1', 'D2',
    'SP1', 'SP2', 'I1', 'I2', 'F1', 'F2', 'B1', 'N1', 'P1', 'T1', 'G1'
)


def season_range(start_date, end_date):
    for n in range(end_date.year - start_date.year + 1):
        year = start_date.replace(year=start_date.year + n).strftime('%y')
        year_plus_one = start_date.replace(year=start_date.year + n + 1).strftime('%y')
        yield f'{year}{year_plus_one}'


class Dataset:
    base_url = 'https://www.football-data.co.uk/mmz4281/'

    def __init__(self, league, season, file_path=None):
        
        self.league = league
        self.season = season
        self.file_path = Path(file_path) if file_path is not None else Path(f'data/{self.league}/{season}.csv')
        self.data = None

    def download(self):
        url = f'{self.base_url}{season}/{league}.csv'
        self.data = pd.read_csv(url)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(self.file_path)

    def upload(self):
        pass


end_date = datetime.today().date()
for league in LEAGUES:
    for season in season_range(START_DATE, end_date):
        ds = Dataset(league, season)
        if ds.file_path.is_file():
            continue
        ds.download()
        print()

print()