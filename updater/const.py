from datetime import datetime

FOOTBALL_CO_UK_CONFIG = {
    'start_date': datetime(2000, 1, 1).date(),
    'leagues': (
        'E0', 'E1', 'E2', 'E3', 'EC', 'SC0', 'SC1', 'SC2', 'SC3', 'D1', 'D2',
        'SP1', 'SP2', 'I1', 'I2', 'F1', 'F2', 'B1', 'N1', 'P1', 'T1', 'G1'
    ),
    'column_names': {
        'Div': 'league',
        'Date': 'match_date',
        'Time': 'match_time',
        'HomeTeam': 'home_team',
        'AwayTeam': 'away_team',
        'FTHG': 'home_score',
        'FTAG': 'away_score', 
        'FTR': 'match_result',
        'HTHG': 'home_ht_score',
        'HTAG': 'away_ht_score', 
        'HTR': 'ht_result', 
        'HS': 'home_shots', 
        'AS': 'away_shots', 
        'HST': 'home_shots_ot', 
        'AST': 'away_shots_ot',
        'HF': 'home_fouls',
        'AF': 'away_fouls',
        'HC': 'home_corners', 
        'AC': 'away_corners', 
        'HY': 'home_yellow', 
        'AY': 'away_yellow', 
        'HR': 'home_red', 
        'AR': 'away_red',
        'MaxCH': 'maxch',
        'MaxCD': 'maxcd',
        'MaxCA': 'maxca',
        'AvgCH': 'avgch',
        'AvgCD': 'avgcd',
        'AvgCA': 'avgca',
        'MaxC>2.5': 'maxc_over', 
        'MaxC<2.5': 'maxc_under',
        'AvgC>2.5': 'avgc_over', 
        'AvgC<2.5': 'avgc_under'
    },
    'columns_select': [
        'Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR',
        'HTHG', 'HTAG', 'HTR', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 
        'AC', 'HY', 'AY', 'HR', 'AR', 'MaxCH', 'MaxCD', 'MaxCA', 'AvgCH',
        'AvgCD', 'AvgCA', 'MaxC>2.5', 'MaxC<2.5', 'AvgC>2.5', 'AvgC<2.5'
    ],
    'validation': {
        'rows': {
            'threshold': 0.5
        },
        'columns': {
            'valid_columns': ['HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
        },
        'valid_dtypes': {
            'FTHG': 'int64',
            'FTAG': 'int64', 
            'HTHG': 'int64',
            'HTAG': 'int64',  
            'HS': 'int64', 
            'AS': 'int64', 
            'HST': 'int64', 
            'AST': 'int64',
            'HF': 'int64',
            'AF': 'int64',
            'HC': 'int64', 
            'AC': 'int64', 
            'HY': 'int64', 
            'AY': 'int64', 
            'HR': 'int64', 
            'AR': 'int64',
            'MaxCH': 'float64',
            'MaxCD': 'float64',
            'MaxCA': 'float64',
            'AvgCH': 'float64',
            'AvgCD': 'float64',
            'AvgCA': 'float64',
            'MaxC>2.5': 'float64', 
            'MaxC<2.5': 'float64',
            'AvgC>2.5': 'float64', 
            'AvgC<2.5': 'float64'
        }
    }
}
