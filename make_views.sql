-- cvterm.name should be unique right?
CREATE VIEW stock_date_plant (stock_id, plant_date) AS
    SELECT stock_id, value
        FROM stockprop
        WHERE type_id = (
            SELECT cvterm_id FROM cvterm WHERE name = 'plant_date'
        )

CREATE VIEW stock_date_pick (stock_id, pick_date) AS
    SELECT stock_id, value
        FROM stockprop
        WHERE type_id = (
            SELECT cvterm_id FROM cvterm WHERE name = 'pick_date'
        )
