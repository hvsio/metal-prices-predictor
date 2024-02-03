CREATE VIEW metals_analytics.metals_training_data AS
SELECT * FROM metals_analytics.metal_prices as mp
WHERE mp.timestamp BETWEEN NOW() - INTERVAL '12 HOURS' AND NOW()
