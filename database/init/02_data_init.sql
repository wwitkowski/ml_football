CREATE SCHEMA IF NOT EXISTS football_data;

CREATE TABLE IF NOT EXISTS football_data.football_data_co_uk (
	match_id SERIAL PRIMARY KEY,
	league varchar(50) NOT NULL,
	season varchar(10) NOT NULL,
	match_date date NOT NULL,
	match_time time NULL,
	home_team varchar(50) NOT NULL,
	away_team varchar(50) NOT NULL,
	home_score int2 NOT NULL,
	away_score int2 NOT NULL,
	match_result varchar(1) NOT NULL,
	home_ht_score int2 NULL,
	away_ht_score int2 NULL,
	ht_result varchar(1) NULL,
	home_shots int2 NULL,
	away_shots int2 NULL,
	home_shots_ot int2 NULL,
	away_shots_ot int2 NULL,
	home_corners int2 NULL,
	away_corners int2 NULL,
	home_fouls int2 NULL,
	away_fouls int2 NULL,
	home_yellow int2 NULL,
	away_yellow int2 NULL,
	home_red int2 NULL,
	away_red int2 NULL,
	maxh numeric(5, 2) NULL,
	maxd numeric(5, 2) NULL,
	maxa numeric(5, 2) NULL,
	avgh numeric(5, 2) NULL,
	avgd numeric(5, 2) NULL,
	avga numeric(5, 2) NULL,
	max_over numeric(5, 2) NULL,
	max_under numeric(5, 2) NULL,
	avg_over numeric(5, 2) NULL,
	avg_under numeric(5, 2) NULL,
	CONSTRAINT fd_unique_match UNIQUE (season, league, match_date, home_team, away_team)
);

GRANT USAGE ON SCHEMA football_data TO mlfootball_api;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA football_data TO mlfootball_api;