-- 1. Club analysis
-- Overall win/draw/loss from all the clubs (add for each club)
SELECT 
  CASE 
    WHEN is_win = 1 THEN 'Win'
    WHEN own_goals = opponent_goals THEN 'Draw'
    ELSE 'Loss'
  END AS result,
  COUNT(*) AS num_games
FROM club_games
GROUP BY result;

-- Win rate by clubs (Maybe add the club name)
SELECT 
  club_id,
  COUNT(*) AS total_games,
  SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) AS total_wins,
  ROUND(SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate
FROM club_games
GROUP BY club_id
ORDER BY win_rate DESC;

-- Home vs. Away Performance (wine rate) - Might IMP
SELECT 
  club_id,
  hosting,
  COUNT(*) AS games,
  SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) AS wins,
  ROUND(SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate
FROM club_games
GROUP BY club_id, hosting
ORDER BY club_id, hosting;

-- Performance Against top 5 Opponents (win rate) - IMP if focus on few clubs
SELECT 
  club_id,
  COUNT(*) AS games_against_top_5,
  SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) AS wins_against_top_5
FROM club_games
WHERE opponent_position <= 5
GROUP BY club_id
ORDER BY wins_against_top_5 DESC;

-- Average goal difference per game for each club (ranks from highest to lowest)
SELECT 
  club_id,
  ROUND(AVG(CAST(own_goals AS SIGNED) - CAST(opponent_goals AS SIGNED)), 2) AS avg_goal_diff
FROM club_games
GROUP BY club_id
ORDER BY avg_goal_diff DESC;

-- Most goals by club across all games
SELECT 
    c.name,
    COUNT(*) AS total_goals
FROM game_events_staging ge
JOIN clubs_staging c ON ge.club_id = c.club_id
WHERE ge.type = 'Goals'
GROUP BY c.name
ORDER BY total_goals DESC
LIMIT 10;

-- How much clubs rotate their starting lineup (Might remove this one)
SELECT 
    c.name AS Club_name,
    COUNT(DISTINCT gl.player_id) AS unique_starters
FROM game_lineups_staging gl
JOIN clubs_staging c ON gl.club_id = c.club_id
WHERE gl.type = 'starting_lineup'
GROUP BY c.name
ORDER BY unique_starters DESC
LIMIT 10;

-- Top 10 net-spending clubs (total transfers) - IMP
SELECT 
    c.name,
    SUM(CASE WHEN t.to_club_id = c.club_id THEN t.transfer_fee ELSE 0 END) AS total_spent,
    SUM(CASE WHEN t.from_club_id = c.club_id THEN t.transfer_fee ELSE 0 END) AS total_earned,
    SUM(CASE WHEN t.to_club_id = c.club_id THEN t.transfer_fee ELSE 0 END) -
    SUM(CASE WHEN t.from_club_id = c.club_id THEN t.transfer_fee ELSE 0 END) AS net_spend
FROM clubs_staging c
LEFT JOIN transfers_staging t ON c.club_id IN (t.to_club_id, t.from_club_id)
GROUP BY c.name
ORDER BY net_spend DESC
LIMIT 10;

-- Club performance by season - IMP
SELECT 
    c.name,
    g.season,
    SUM(CASE WHEN cg.is_win = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN cg.is_win = 'loss' THEN 0 ELSE 1 END) AS losses
FROM club_games_staging cg
JOIN clubs_staging c ON cg.club_id = c.club_id
JOIN games_staging g ON cg.game_id = g.game_id
GROUP BY c.name, g.season
ORDER BY g.season, c.name;

SELECT *
FROM club_games_staging;

-- Player analysis

-- Position player distribution
SELECT position, COUNT(*) 
FROM players_staging
GROUP BY position;

-- Average market value per year
SELECT YEAR(date), AVG(market_value_in_eur) 
FROM player_valuations_staging
GROUP BY YEAR(date);

-- Average player performanace (minutes, yellow card, red card) - IMP
SELECT player_id, AVG(minutes_played), AVG(yellow_cards), AVG(red_cards)
FROM appearances_staging 
GROUP BY player_id;

-- Average player performanace (minutes, goals, assists) - IMP
SELECT player_id, AVG(minutes_played), AVG(goals), AVG(assists)
FROM appearances_staging 
GROUP BY player_id;

-- Average transfer market spend per year 
SELECT YEAR(transfer_date), SUM(transfer_fee) 
FROM transfers_staging
GROUP BY YEAR(transfer_date);

-- Top 10 Goal-Scoring players and their clubs (Make it for the last 3 seasons - IMP)

SELECT DISTINCT
    p.name AS player_name,
    c.name AS club_name,
    COUNT(*) AS total_goals
FROM game_events_staging ge
JOIN players_staging p ON TRIM(ge.player_id) = TRIM(p.player_id)
JOIN clubs_staging c ON TRIM(ge.club_id) = TRIM(c.club_id)
WHERE ge.type = 'Goals'
GROUP BY p.name, c.name
ORDER BY total_goals DESC
LIMIT 10;

-- Average player market value by position (IMP)
SELECT 
    p.position,
    ROUND(AVG(pv.market_value_in_eur), 2) AS avg_value
FROM player_valuations_staging pv
JOIN players_staging p ON pv.player_id = p.player_id
GROUP BY p.position
ORDER BY avg_value DESC;

-- Player goal efficiency (Goals per Minute) (add season) - NO IMP

SELECT 
    p.name AS player_name,
    SUM(CASE WHEN ge.type = 'Goals' THEN 1 ELSE 0 END) AS goals,
    SUM(a.minutes_played) AS minutes_played,
    ROUND(SUM(CASE WHEN ge.type = 'Goals' THEN 1 ELSE 0 END) / SUM(a.minutes_played), 4) AS goals_per_minute
FROM appearances_staging a
JOIN players p ON a.player_id = p.player_id
LEFT JOIN game_events_staging ge ON a.player_id = ge.player_id AND a.game_id = ge.game_id
GROUP BY p.name
HAVING minutes_played > 300
ORDER BY goals_per_minute DESC
LIMIT 10;


-- 20 players with most red cards - IMP
SELECT 
    p.name AS player_name,
    SUM(a.red_cards) AS red_cards
FROM appearances_staging a
JOIN players_staging p ON a.player_id = p.player_id
GROUP BY p.name
HAVING red_cards > 0
ORDER BY red_cards DESC
LIMIT 20;

-- Most impactful players (Goals + Assists) - IMP last 3 seasons
SELECT 
  p.name AS player_name,
  SUM(a.goals) AS total_goals,
  SUM(a.assists) AS total_assists,
  SUM(a.goals + a.assists) AS total_contributions
FROM appearances_staging a
JOIN players_staging p ON a.player_id = p.player_id
GROUP BY p.name
ORDER BY total_contributions DESC
LIMIT 10;

-- Players with more than 5 goals and avg value under €2M - IMP last 3 seasons and age
SELECT p.name AS player_name,
       COUNT(ge.game_event_id) AS goals,
       ROUND(AVG(pv.market_value_in_eur), 2) AS avg_value
FROM players_staging p
JOIN game_events_staging ge ON p.player_id = ge.player_id AND ge.type = 'goals'
JOIN player_valuations_staging pv ON p.player_id = pv.player_id
GROUP BY p.name
HAVING goals > 5 AND avg_value < 2000000
ORDER BY goals DESC;

-- Efficiency: goals per 90 minutes - IMP last 3 season
SELECT 
  p.name AS player_name,
  SUM(a.goals) AS goals,
  SUM(a.minutes_played) AS minutes,
  ROUND((SUM(a.goals) / SUM(a.minutes_played)) * 90, 2) AS goals_per_90
FROM appearances_staging a
JOIN players_staging p ON a.player_id = p.player_id
GROUP BY p.name
HAVING minutes > 300
ORDER BY goals_per_90 DESC
LIMIT 20;

-- Most consistent performers (Goals in Most Games)
SELECT 
  p.name AS player_name,
  COUNT(DISTINCT ge.game_event_id) AS scoring_games
FROM game_events_staging ge
JOIN players_staging p ON ge.player_id = p.player_id
WHERE ge.type = 'Goals'
GROUP BY p.name
ORDER BY scoring_games DESC
LIMIT 20;

-- Players With Most Assists - IMP last 3 season
SELECT 
  p.name AS player_name,
  SUM(a.assists) AS assists
FROM appearances_staging a
JOIN players p ON a.player_id = p.player_id
GROUP BY p.name
ORDER BY assists DESC
LIMIT 10;

-- Overused, Underperforming Players - NO
SELECT 
  p.name AS player_name,
  COUNT(*) AS games_played,
  SUM(a.goals) AS total_goals
FROM appearances_staging a
JOIN players_staging p ON a.player_id = p.player_id
GROUP BY p.name
HAVING games_played >= 10 AND total_goals = 0
ORDER BY games_played DESC
LIMIT 20;


-- Game analysis

-- Number of games per competition type
SELECT DISTINCT competition_type, COUNT(*) 
FROM games_staging 
GROUP BY competition_type;

-- Number of games per season
SELECT season, COUNT(*) 
FROM games_staging 
GROUP BY season;

-- Average goals per game for each season
SELECT 
    g.season,
    ROUND(COUNT(ge.game_event_id) / COUNT(DISTINCT g.game_id), 2) AS avg_goals_per_game
FROM game_events_staging ge
JOIN games_staging g ON ge.game_id = g.game_id
WHERE ge.type = 'Goals'
GROUP BY g.season
ORDER BY g.season;

-- Competition with highest avg goals per game
SELECT 
  comp.name AS competition,
  ROUND(COUNT(ge.game_event_id) / COUNT(DISTINCT g.game_id), 2) AS avg_goals_per_game
FROM game_events_staging ge
JOIN games_staging g ON ge.game_id = g.game_id
JOIN competitions_staging comp ON g.competition_id = comp.competition_id
WHERE ge.type = 'Goals'
GROUP BY comp.name
ORDER BY avg_goals_per_game DESC;

-- Total goals, average goals per match, and win rate - IMP last 3 years
SELECT 
  g.home_club_name,
  COUNT(*) AS games_played,
  SUM(cg.own_goals) AS total_goals,
  ROUND(AVG(cg.own_goals), 2) AS avg_goals_per_game,
  SUM(cg.is_win) AS total_wins,
  ROUND(SUM(cg.is_win) / COUNT(*), 2) AS win_rate
FROM club_games_staging cg
JOIN games_staging g ON cg.game_id = g.game_id
WHERE cg.club_id = g.home_club_id
GROUP BY g.home_club_name
ORDER BY total_goals DESC
LIMIT 10;





