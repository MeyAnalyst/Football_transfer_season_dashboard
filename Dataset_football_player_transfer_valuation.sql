SELECT 
    t.player_id,
    p.name AS player_name,
    t.transfer_date,
    t.transfer_fee,
    pv.market_value_in_eur,
    (pv.market_value_in_eur - t.transfer_fee) AS value_gap,
    ROUND(t.transfer_fee / NULLIF(pv.market_value_in_eur, 0), 2) AS fee_to_value_ratio,
    CASE
        WHEN pv.market_value_in_eur IS NULL OR t.transfer_fee IS NULL THEN 'Unknown'
        WHEN ROUND(t.transfer_fee / NULLIF(pv.market_value_in_eur, 0), 2) = 0 THEN 'No fee/value'
        WHEN t.transfer_fee < pv.market_value_in_eur THEN 'Underpaid'
        WHEN t.transfer_fee > pv.market_value_in_eur THEN 'Overpaid'
        ELSE 'Fair Price'
    END AS status
FROM transfers_staging t
JOIN player_valuations_staging pv 
  ON t.player_id = pv.player_id AND YEAR(t.transfer_date) = YEAR(pv.date)
JOIN players_staging p ON t.player_id = p.player_id
WHERE t.transfer_date BETWEEN '2021-01-01' AND '2025-12-31';
