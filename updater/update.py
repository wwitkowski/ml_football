import os
import sys
import time
import logging
from datetime import datetime
from collections import defaultdict

from pathlib import Path

from const import FOOTBALL_CO_UK_CONFIG
from dataset import Dataset

file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

try:
    sys.path.remove(str(parent))
except ValueError: # Already removed
    pass

from utils.postgres import PGDatabase


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def season_range(start_date, end_date):
    for n in range(end_date.year - start_date.year + 1):
        year = start_date.replace(year=start_date.year + n).strftime('%y')
        year_plus_one = start_date.replace(year=start_date.year + n + 1).strftime('%y')
        yield f'{year}{year_plus_one}'


def update():
    end_date = datetime.today().date()
    pg = PGDatabase(
        host=os.getenv('POSTGRES_HOST'),
        database='ml_football',
        user='postgres',
        password=os.getenv('POSTGRES_PASSWORD')
    )
    uploaded_seasons = pg.fetch('SELECT DISTINCT season, league FROM match')
    season_dict = defaultdict(list)
    for season, league in uploaded_seasons:
        season_dict[league].append(season)
    for league in FOOTBALL_CO_UK_CONFIG['leagues']:
        for season in season_range(FOOTBALL_CO_UK_CONFIG['start_date'], end_date):
            if season in season_dict[league] and season != max(season_dict[league]):
                continue
            dataset = Dataset(
                url=f"{FOOTBALL_CO_UK_CONFIG['base_url']}{season}/{league}.csv",
                file_path=f'data/{league}/{season}.csv',
                validation_config=FOOTBALL_CO_UK_CONFIG['validation']
            )
            dataset.load(overwrite=True)
            if dataset.data is None or not dataset.validated:
                continue
            dataset.data = dataset.data[
                [col for col in dataset.data.columns if col in FOOTBALL_CO_UK_CONFIG['columns_select']]
            ]
            dataset.parse_dates('Date', date_formats=['%d/%m/%y', '%d/%m/%Y'])
            dataset.data.rename(columns=FOOTBALL_CO_UK_CONFIG['column_names'], inplace=True)
            dataset.data = dataset.data.assign(season=season)
            pg.execute(f"DELETE FROM match WHERE league = '{league}' AND season = '{season}'")
            dataset.upload(pg.engine, 'match', if_exists='append', index=False)
            time.sleep(1)


if __name__ == '__main__':
    update()
