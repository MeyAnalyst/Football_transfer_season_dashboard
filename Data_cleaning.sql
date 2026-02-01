-- Data Cleaning

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any rows and columns

CREATE TABLE appearances_staging
LIKE appearances;
SELECT*
FROM appearances_staging;
INSERT appearances_staging
SELECT *
FROM appearances;

CREATE TABLE club_games_staging
LIKE club_games;
SELECT*
FROM club_games_staging;
INSERT club_games_staging
SELECT *
FROM club_games;

CREATE TABLE clubs_staging
LIKE clubs;
SELECT*
FROM clubs_staging;
INSERT clubs_staging
SELECT *
FROM clubs;

CREATE TABLE competitions_staging
LIKE competitions;
SELECT*
FROM competitions_staging;
INSERT competitions_staging
SELECT *
FROM competitions;

CREATE TABLE game_events_staging
LIKE game_events;
SELECT*
FROM game_events_staging;
INSERT game_events_staging
SELECT *
FROM game_events;

CREATE TABLE game_lineups_staging
LIKE game_lineups;
SELECT*
FROM game_lineups_staging;
INSERT game_lineups_staging
SELECT *
FROM game_lineups;

CREATE TABLE games_staging
LIKE games;
SELECT*
FROM games_staging;
INSERT games_staging
SELECT *
FROM games;

CREATE TABLE players_staging
LIKE players;
SELECT*
FROM players_staging;
INSERT players_staging
SELECT *
FROM players;

CREATE TABLE player_valuations_staging
LIKE player_valuations;
SELECT*
FROM player_valuations_staging;
INSERT player_valuations_staging
SELECT *
FROM player_valuations;

CREATE TABLE transfers_staging
LIKE transfers;
SELECT*
FROM transfers_staging;
INSERT transfers_staging
SELECT *
FROM transfers;

-- 1. Remove duplicates
-- 1.1. Remove duplicates appearances_staging

SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
appearance_id, 
game_id, 
player_id, 
player_club_id, 
player_current_club_id, 
`date`, 
player_name, 
competition_id, 
yellow_cards,red_cards,
goals,
assists,
minutes_played) AS row_num
FROM appearances_staging;

WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
appearance_id, 
game_id, 
player_id, 
player_club_id, 
player_current_club_id, 
`date`, 
player_name, 
competition_id, 
yellow_cards,red_cards,
goals,
assists,
minutes_played) AS row_num
FROM appearances_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- 1.2. Remove duplicates club_games_staging
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
game_id,
club_id,
own_goals,
own_position,
own_manager_name,
opponent_id,
opponent_goals,
opponent_position,
opponent_manager_name,
hosting,
is_win) AS row_num
FROM club_games_staging;

WITH duplicate_club_games_cte AS
(
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
game_id,
club_id,
own_goals,
own_position,
own_manager_name,
opponent_id,
opponent_goals,
opponent_position,
opponent_manager_name,
hosting,
is_win) AS row_num
FROM club_games_staging
)
SELECT *
FROM duplicate_club_games_cte
WHERE row_num > 1;

-- 1.3. Remove duplicates clubs_staging

SELECT club_id, club_code, COUNT(*) 
FROM clubs_staging
GROUP BY club_id, club_code
HAVING COUNT(*) > 1;

-- 1.4. Remove duplicates competitions_staging

SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
competition_id,
competition_code,
`name`,
sub_type,
`type`,
country_id,
country_name,
domestic_league_code,
confederation,
url,
is_major_national_league
) AS row_num
FROM competitions_staging;

WITH duplicate_competitions_cte AS
(
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
competition_id,
competition_code,
`name`,
sub_type,
`type`,
country_id,
country_name,
domestic_league_code,
confederation,
url,
is_major_national_league) AS row_num
FROM competitions_staging
)
SELECT *
FROM duplicate_competitions_cte
WHERE row_num > 1;

-- 1.5. Remove duplicates game_events_staging

SELECT game_event_id,game_id, COUNT(*) 
FROM game_events_staging
GROUP BY game_event_id,game_id
HAVING COUNT(*) > 1;

-- 1.6. Remove duplicates game_lineups_staging

SELECT game_lineups_id,game_id, COUNT(*) 
FROM game_lineups_staging
GROUP BY game_lineups_id,game_id
HAVING COUNT(*) > 1;

-- 1.7. Remove duplicates games_staging

SELECT game_id,competition_id, COUNT(*) 
FROM games_staging
GROUP BY game_id,competition_id
HAVING COUNT(*) > 1;

-- 1.8. Remove duplicates players_staging

SELECT player_id,`name`, COUNT(*) 
FROM players_staging
GROUP BY player_id,`name`
HAVING COUNT(*) > 1;

-- 1.9. Remove duplicates players_staging

SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
player_id,
`date`,
market_value_in_eur,
current_club_id,
player_club_domestic_competition_id
) AS row_num
FROM player_valuations_staging;

WITH duplicate_player_valuations_cte AS
(
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
player_id,
`date`,
market_value_in_eur,
current_club_id,
player_club_domestic_competition_id) AS row_num
FROM player_valuations_staging
)
SELECT *
FROM duplicate_player_valuations_cte
WHERE row_num > 1;

-- 1.10. Remove duplicates transfers_staging

SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
player_id,
transfer_date,
transfer_season,
from_club_id,
to_club_id,
from_club_name,
to_club_name,
transfer_fee,
market_value_in_eur,
player_name) AS row_num
FROM transfers_staging;

WITH duplicate_transfers_cte AS
(
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY 
player_id,
transfer_date,
transfer_season,
from_club_id,
to_club_id,
from_club_name,
to_club_name,
transfer_fee,
market_value_in_eur,
player_name) AS row_num
FROM transfers_staging
)
SELECT *
FROM duplicate_transfers_cte
WHERE row_num > 1;

-- 2. Standardize the data
-- 2.1 TRIM

SELECT *, 
TRIM(player_id),
TRIM(`date`),
TRIM(market_value_in_eur),
TRIM(current_club_id),
TRIM(player_club_domestic_competition_id)
FROM player_valuations_staging;

UPDATE player_valuations_staging
SET player_id = TRIM(player_id),
	`date` = TRIM(`date`),
	market_value_in_eur = TRIM(market_value_in_eur),
	current_club_id = TRIM(current_club_id),
	player_club_domestic_competition_id = TRIM(player_club_domestic_competition_id);
    
-- 2.2 Charcter check
    
SELECT DISTINCT hosting
FROM club_games_staging
ORDER BY 1;

SELECT DISTINCT country_name
FROM competitions_staging
ORDER BY 1;

SELECT DISTINCT sub_type
FROM competitions_staging
ORDER BY 1;

SELECT DISTINCT `type`
FROM competitions_staging
ORDER BY 1;

SELECT DISTINCT confederation
FROM competitions_staging
ORDER BY 1;

SELECT DISTINCT `type`
FROM game_events_staging
ORDER BY 1;

SELECT DISTINCT `type`
FROM game_lineups_staging
ORDER BY 1;

SELECT DISTINCT competition_type
FROM games_staging
ORDER BY 1;

SELECT DISTINCT position
FROM game_lineups_staging
ORDER BY 1;


SELECT DISTINCT country_of_birth
FROM players_staging
ORDER BY 1;

SELECT *
FROM players_staging
WHERE country_of_birth LIKE '%Congo%';

UPDATE players_staging
SET country_of_birth = 'Congo'
WHERE country_of_birth LIKE '%Congo%';

SELECT *
FROM players_staging
WHERE country_of_birth LIKE 'Turkiye';

UPDATE players_staging
SET country_of_birth = 'Turkey'
WHERE country_of_birth LIKE 'Turkiye';

SELECT DISTINCT country_of_citizenship
FROM players_staging
ORDER BY 1;

SELECT *
FROM players_staging
WHERE country_of_citizenship LIKE '%Congo%';

UPDATE players_staging
SET country_of_citizenship = 'Congo'
WHERE country_of_citizenship LIKE '%Congo%';

SELECT *
FROM players_staging
WHERE country_of_citizenship LIKE 'Turkiye';

UPDATE players_staging
SET country_of_citizenship = 'Turkey'
WHERE country_of_citizenship LIKE 'Turkiye';

SELECT DISTINCT sub_position
FROM players_staging
ORDER BY 1;

SELECT DISTINCT position
FROM players_staging
ORDER BY 1;

SELECT DISTINCT foot
FROM players_staging
ORDER BY 1;

SELECT DISTINCT from_club_name
FROM transfers_staging
ORDER BY 1;

SELECT DISTINCT to_club_name
FROM transfers_staging
ORDER BY 1;

-- 2.3 From text to date

SELECT *
FROM appearances_staging;

SELECT `date`,
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM appearances_staging;

UPDATE appearances_staging
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE appearances_staging
MODIFY COLUMN `date` DATE;

SELECT *
FROM game_events_staging;

SELECT `date`,
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM game_events_staging;

UPDATE game_events_staging
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE game_events_staging
MODIFY COLUMN `date` DATE;

SELECT *
FROM game_lineups_staging;

SELECT `date`,
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM game_lineups_staging;

UPDATE game_lineups_staging
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE game_lineups_staging
MODIFY COLUMN `date` DATE;

SELECT *
FROM games_staging;

SELECT `date`,
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM games_staging;

UPDATE games_staging
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE games_staging
MODIFY COLUMN `date` DATE;

SELECT *
FROM players_staging;

SELECT date_of_birth, TRIM(TRAILING '00:00:00' FROM date_of_birth)
FROM players_staging;

UPDATE players_staging
SET date_of_birth = TRIM(TRAILING '00:00:00' FROM date_of_birth);

SELECT date_of_birth,
STR_TO_DATE(date_of_birth, '%Y-%m-%d')
FROM players_staging;

UPDATE players_staging
SET date_of_birth = STR_TO_DATE(date_of_birth, '%Y-%m-%d');

ALTER TABLE players_staging
MODIFY COLUMN date_of_birth DATE;

SELECT *
FROM player_valuations_staging;

SELECT `date`,
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM player_valuations_staging;

UPDATE player_valuations_staging
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE player_valuations_staging
MODIFY COLUMN `date` DATE;

SELECT *
FROM transfers_staging;

SELECT transfer_date,
STR_TO_DATE(transfer_date, '%Y-%m-%d')
FROM transfers_staging;

UPDATE transfers_staging
SET transfer_date = STR_TO_DATE(transfer_date, '%Y-%m-%d');

ALTER TABLE transfers_staging
MODIFY COLUMN transfer_date DATE;

-- 2.4. Convert a string (text) to an integer

SELECT *
FROM club_games_staging;

SELECT own_goals
FROM club_games_staging
WHERE own_goals NOT REGEXP '^[0-9]+$';

SELECT own_goals, own_position, opponent_goals, opponent_position
FROM club_games_staging
WHERE own_goals = '' OR
own_position = ''OR
opponent_goals = ''OR
opponent_position= '';

UPDATE club_games_staging
SET 
    own_goals = NULLIF(own_goals, ''),
    own_position = NULLIF(own_position, ''),
    opponent_goals = NULLIF(opponent_goals, ''),
    opponent_position = NULLIF(opponent_position, '')
WHERE 
    own_goals = '' OR
    own_position = '' OR
    opponent_goals = '' OR
    opponent_position = '';
    
SELECT own_goals, own_position, opponent_goals, opponent_position,
CONVERT(own_goals, UNSIGNED INTEGER),
CONVERT(own_position, UNSIGNED INTEGER),
CONVERT(opponent_goals, UNSIGNED INTEGER),
CONVERT(opponent_position, UNSIGNED INTEGER)
FROM club_games_staging;

UPDATE club_games_staging
SET 
    own_goals = CONVERT(own_goals, UNSIGNED INTEGER),
    own_position = CONVERT(own_position, UNSIGNED INTEGER),
    opponent_goals = CONVERT(opponent_goals, UNSIGNED INTEGER),
    opponent_position = CONVERT(opponent_position, UNSIGNED INTEGER);

ALTER TABLE club_games_staging 
MODIFY COLUMN own_goals INT,
MODIFY COLUMN own_position INT,
MODIFY COLUMN opponent_goals INT,
MODIFY COLUMN opponent_position INT;

SELECT *
FROM clubs_staging;

SELECT net_transfer_record,
  CASE
    WHEN LOWER(net_transfer_record) LIKE '%m' THEN
      CAST(REPLACE(REPLACE(REPLACE(net_transfer_record, '€', ''), '+', ''), 'm', '') AS DECIMAL(10,0)) * 1000000

    WHEN LOWER(net_transfer_record) LIKE '%k' THEN
      CAST(REPLACE(REPLACE(REPLACE(net_transfer_record, '€', ''), '+', ''), 'k', '') AS DECIMAL(10,0)) * 1000

    WHEN net_transfer_record = '+-0' THEN 0

    ELSE CAST(REPLACE(REPLACE(net_transfer_record, '€', ''), '+', '') AS DECIMAL(10,0))
  END AS standardised_value
FROM clubs_staging;

UPDATE clubs_staging
SET net_transfer_record = CASE
    WHEN LOWER(net_transfer_record) LIKE '%m' THEN
        CAST(REPLACE(REPLACE(REPLACE(net_transfer_record, '€', ''), '+', ''), 'm', '') AS DECIMAL(10,0)) * 1000000

    WHEN LOWER(net_transfer_record) LIKE '%k' THEN
        CAST(REPLACE(REPLACE(REPLACE(net_transfer_record, '€', ''), '+', ''), 'k', '') AS DECIMAL(10,0)) * 1000

    WHEN net_transfer_record = '+-0' THEN 0

    ELSE CAST(REPLACE(REPLACE(net_transfer_record, '€', ''), '+', '') AS DECIMAL(10,2))
END;

SELECT net_transfer_record,
CONVERT(net_transfer_record, SIGNED INTEGER)
FROM clubs_staging;

UPDATE clubs_staging
SET net_transfer_record = CONVERT(net_transfer_record, SIGNED INTEGER);

SELECT net_transfer_record
FROM clubs_staging
WHERE net_transfer_record NOT REGEXP '^[0-9]+$';

ALTER TABLE clubs_staging 
MODIFY COLUMN net_transfer_record BIGINT;

SELECT *
FROM games_staging;

SELECT attendance
FROM games_staging
WHERE attendance NOT REGEXP '^[0-9]+$';

SELECT attendance
FROM games_staging
WHERE attendance = 'None';

UPDATE games_staging
SET attendance = NULL
WHERE attendance = 'None';

SELECT attendance,
CONVERT(attendance, UNSIGNED INTEGER)
FROM games_staging;

UPDATE games_staging
SET attendance = CONVERT(attendance, UNSIGNED INTEGER);

ALTER TABLE games_staging 
MODIFY COLUMN attendance INT;

SELECT *
FROM games_staging;

SELECT home_club_goals, away_club_goals, home_club_position, away_club_position
FROM games_staging
WHERE home_club_goals NOT REGEXP '^[0-9]+$' OR
away_club_goals NOT REGEXP '^[0-9]+$'OR
home_club_position NOT REGEXP '^[0-9]+$'OR
away_club_position NOT REGEXP '^[0-9]+$';

SELECT home_club_position, away_club_position
FROM games_staging
WHERE 
home_club_goals = 'None' OR
away_club_goals = 'None'OR 
home_club_position = 'None' OR
away_club_position = 'None';

UPDATE games_staging
SET 
    home_club_goals = NULLIF(home_club_goals, 'None'),
    away_club_goals = NULLIF(away_club_goals, 'None'),
    home_club_position = NULLIF(home_club_position, 'None'),
    away_club_position = NULLIF(away_club_position, 'None')
WHERE 
home_club_goals = 'None' OR
away_club_goals = 'None'OR 
home_club_position = 'None' OR
away_club_position = 'None';
    
SELECT home_club_goals, away_club_goals, home_club_position, away_club_position,
CONVERT(home_club_goals, UNSIGNED INTEGER),
CONVERT(away_club_goals, UNSIGNED INTEGER),
CONVERT(home_club_position, UNSIGNED INTEGER),
CONVERT(away_club_position, UNSIGNED INTEGER)
FROM games_staging;

UPDATE games_staging
SET 
    home_club_goals = CONVERT(home_club_goals, UNSIGNED INTEGER),
    away_club_goals = CONVERT(away_club_goals, UNSIGNED INTEGER),
    home_club_position = CONVERT(home_club_position, UNSIGNED INTEGER),
    away_club_position = CONVERT(away_club_position, UNSIGNED INTEGER);

ALTER TABLE games_staging
MODIFY COLUMN home_club_goals INT,
MODIFY COLUMN away_club_goals INT,
MODIFY COLUMN home_club_position INT,
MODIFY COLUMN away_club_position INT;



SELECT *
FROM players_staging;

SELECT height_in_cm, market_value_in_eur, highest_market_value_in_eur
FROM players_staging
WHERE height_in_cm NOT REGEXP '^[0-9]+$'OR
market_value_in_eur NOT REGEXP '^[0-9]+$'OR
highest_market_value_in_eur NOT REGEXP '^[0-9]+$';

SELECT height_in_cm, market_value_in_eur, highest_market_value_in_eur
FROM players_staging
WHERE height_in_cm = '' OR
market_value_in_eur = ''OR
highest_market_value_in_eur = '';

UPDATE players_staging
SET 
    height_in_cm = NULLIF(height_in_cm, ''),
    market_value_in_eur = NULLIF(market_value_in_eur, ''),
    highest_market_value_in_eur = NULLIF(highest_market_value_in_eur, '')
WHERE 
    height_in_cm = '' OR
    market_value_in_eur = '' OR
    highest_market_value_in_eur = '';
    
SELECT height_in_cm, market_value_in_eur, highest_market_value_in_eur,
CONVERT(height_in_cm, UNSIGNED INTEGER),
CONVERT(market_value_in_eur, UNSIGNED INTEGER),
CONVERT(highest_market_value_in_eur, UNSIGNED INTEGER)
FROM players_staging;

UPDATE players_staging
SET 
    height_in_cm = CONVERT(height_in_cm, UNSIGNED INTEGER),
    market_value_in_eur = CONVERT(market_value_in_eur, UNSIGNED INTEGER),
    highest_market_value_in_eur = CONVERT(highest_market_value_in_eur, UNSIGNED INTEGER);

ALTER TABLE players_staging 
MODIFY COLUMN height_in_cm INT,
MODIFY COLUMN market_value_in_eur INT,
MODIFY COLUMN highest_market_value_in_eur INT;

SELECT *
FROM transfers_staging;

SELECT transfer_fee, market_value_in_eur
FROM transfers_staging
WHERE transfer_fee NOT REGEXP '^[0-9]+$'OR
market_value_in_eur NOT REGEXP '^[0-9]+$';

SELECT transfer_fee, market_value_in_eur
FROM transfers_staging
WHERE transfer_fee = '' OR
market_value_in_eur = '';

UPDATE transfers_staging
SET 
    transfer_fee = NULLIF(transfer_fee, ''),
    market_value_in_eur = NULLIF(market_value_in_eur, '')
WHERE 
    transfer_fee = '' OR
    market_value_in_eur = '';
    
SELECT transfer_fee, market_value_in_eur,
CONVERT(transfer_fee, UNSIGNED INTEGER),
CONVERT(market_value_in_eur, UNSIGNED INTEGER)
FROM transfers_staging;

UPDATE transfers_staging
SET 
    transfer_fee = CONVERT(transfer_fee, UNSIGNED INTEGER),
    market_value_in_eur = CONVERT(market_value_in_eur, UNSIGNED INTEGER);

ALTER TABLE transfers_staging 
MODIFY COLUMN transfer_fee INT,
MODIFY COLUMN market_value_in_eur INT;

-- 3. Null values or blank values

-- XXXXXXXXX

-- 4. Remove any rows and columns

SELECT *
FROM clubs_staging;

ALTER TABLE clubs_staging
DROP COLUMN total_market_value,
DROP COLUMN filename,
DROP COLUMN url;

SELECT *
FROM competitions_staging;

ALTER TABLE competitions_staging
DROP COLUMN url;

SELECT *
FROM games_staging;

ALTER TABLE games_staging
DROP COLUMN url;

SELECT *
FROM players_staging;

ALTER TABLE players_staging
DROP COLUMN image_url,
DROP COLUMN url;

