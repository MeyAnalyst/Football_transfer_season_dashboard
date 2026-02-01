WITH appearances_by_year AS (
    SELECT 
        player_id,
        YEAR(date) AS year,
        SUM(minutes_played) AS total_minutes,
        SUM(goals) AS total_goals_in_appearances,
        SUM(assists) AS total_assists,
        SUM(yellow_cards) AS total_yellow_cards,
        SUM(red_cards) AS total_red_cards,
        ROUND(SUM(goals) / NULLIF(SUM(minutes_played), 0) * 90, 2) AS goals_per_90,
        COUNT(DISTINCT game_id) AS games_played
    FROM appearances_staging
    WHERE date BETWEEN '2021-01-01' AND '2025-12-31'
    GROUP BY player_id, YEAR(date)
),

transfer_by_year AS (
    SELECT 
        t.player_id,
        YEAR(t.transfer_date) AS year,
        ROUND(AVG(t.transfer_fee), 2) AS avg_transfer_fee
    FROM transfers_staging t
    WHERE t.transfer_date BETWEEN '2021-01-01' AND '2025-12-31'
    GROUP BY t.player_id, YEAR(t.transfer_date)
),

market_value_by_year AS (
    SELECT 
        player_id,
        YEAR(date) AS year,
        ROUND(AVG(market_value_in_eur), 2) AS avg_market_value
    FROM player_valuations_staging
    WHERE date BETWEEN '2021-01-01' AND '2025-12-31'
    GROUP BY player_id, YEAR(date)
),

goals_by_year AS (
    SELECT 
        ge.player_id,
        YEAR(ge.date) AS year,
        COUNT(*) AS total_goals,
        MAX(g.competition_id) AS competition_id,
        MAX(g.round) AS game_name
    FROM game_events_staging ge
    JOIN games g ON ge.game_id = g.game_id
    WHERE LOWER(ge.type) = 'goals' AND ge.date BETWEEN '2021-01-01' AND '2025-12-31'
    GROUP BY ge.player_id, YEAR(ge.date)
)

SELECT 
    ROW_NUMBER() OVER () AS appearance_id,
    p.player_id,
    p.name AS player_name,
    p.position,
    ay.year,
    ay.total_minutes,
    ay.total_goals_in_appearances,
    ay.total_assists,
    ay.total_yellow_cards,
    ay.total_red_cards,
    ay.goals_per_90,
    ay.games_played,
    COALESCE(gb.total_goals, 0) AS total_goals,
    mv.avg_market_value,
    tf.avg_transfer_fee,
    c.name AS competition_name,
    gb.game_name,
    ROUND(AVG(pv.market_value_in_eur)) AS avg_market_value_by_position

FROM appearances_by_year ay
JOIN players_staging p ON ay.player_id = p.player_id
LEFT JOIN goals_by_year gb ON ay.player_id = gb.player_id AND ay.year = gb.year
LEFT JOIN competitions c ON gb.competition_id = c.competition_id
LEFT JOIN transfer_by_year tf ON ay.player_id = tf.player_id AND ay.year = tf.year
LEFT JOIN market_value_by_year mv ON ay.player_id = mv.player_id AND ay.year = mv.year
LEFT JOIN player_valuations_staging pv ON p.player_id = pv.player_id AND YEAR(pv.date) = ay.year
GROUP BY
    p.player_id, p.name, p.position,
    ay.year, ay.total_minutes, ay.total_goals_in_appearances,
    ay.total_assists, ay.total_yellow_cards, ay.total_red_cards,
    ay.goals_per_90, ay.games_played,
    gb.total_goals, gb.game_name, c.name,
    mv.avg_market_value, tf.avg_transfer_fee;
