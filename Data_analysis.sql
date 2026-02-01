-- 1. Top 10 Goal Scorers
SELECT 
    p.name AS player_name, COUNT(*) AS goals
FROM game_events ge
JOIN players_staging p ON ge.player_id = p.player_id
WHERE ge.type = 'Goals'
AND ge.date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY p.name
ORDER BY goals DESC
LIMIT 10;

-- 2. Players with more than 5 goals and avg value under €2M
SELECT p.name AS player_name,
       COUNT(ge.game_event_id) AS goals,
       ROUND(AVG(pv.market_value_in_eur)) AS avg_market_value
FROM players_staging p
JOIN game_events_staging ge ON p.player_id = ge.player_id 
	AND ge.type = 'goals'
	AND ge.date BETWEEN '2021-01-01' AND '2025-12-31'
LEFT JOIN player_valuations_staging pv ON p.player_id = pv.player_id
	AND pv.date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY p.name
HAVING goals > 5 AND avg_market_value < 2000000
ORDER BY goals DESC;

-- 3. Efficiency: goals per 90 minutes
SELECT 
  p.name AS player_name,
  SUM(a.goals) AS goals,
  SUM(a.minutes_played) AS minutes,
  ROUND((SUM(a.goals) / SUM(a.minutes_played)) * 90, 2) AS goals_per_90
FROM appearances_staging a
JOIN players_staging p ON a.player_id = p.player_id
WHERE a.date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY p.name
HAVING minutes > 300
ORDER BY goals_per_90 DESC
LIMIT 20;


-- 4. Average player market value by position
SELECT 
    p.position,
    ROUND(AVG(pv.market_value_in_eur)) AS avg_market_value
FROM player_valuations_staging pv
RIGHT JOIN players_staging p ON pv.player_id = p.player_id
WHERE pv.date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY p.position
ORDER BY avg_market_value DESC;

-- 5. Average player performance (minutes, yellow card, red card, Assist and goals)

SELECT 
    p.player_id,
    p.name AS player_name,
    ROUND(AVG(a.minutes_played), 2) AS avg_minutes,
    ROUND(AVG(a.goals), 2) AS avg_goals,
    ROUND(AVG(a.assists), 2) AS avg_assists,
    ROUND(AVG(a.yellow_cards), 2) AS avg_yellow_cards,
    ROUND(AVG(a.red_cards), 2) AS avg_red_cards
FROM appearances_staging a
JOIN players_staging p ON a.player_id = p.player_id
WHERE a.date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY p.player_id, p.name
ORDER BY avg_minutes DESC;


-- 6. Avg Transfer Fee vs Avg Market Value by Year
SELECT 
  YEAR(t.transfer_date) AS year,
  ROUND(AVG(t.transfer_fee)) AS avg_transfer_fee,
  ROUND(AVG(pv.market_value_in_eur)) AS avg_market_value
FROM transfers_staging t
JOIN player_valuations_staging pv 
  ON t.player_id = pv.player_id 
  AND YEAR(t.transfer_date) = YEAR(pv.date)
WHERE t.transfer_date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY YEAR(t.transfer_date)
ORDER BY year;





