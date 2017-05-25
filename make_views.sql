-- cvterm.name should be unique right?
CREATE MATERIALIZED VIEW stock_date_plant (stock_id, plant_date) AS
    SELECT stock_id, value
        FROM stockprop
        WHERE type_id = (
            SELECT cvterm_id FROM cvterm WHERE name = 'plant_date'
        );

CREATE MATERIALIZED VIEW stock_date_pick (stock_id, pick_date) AS
    SELECT stock_id, value
        FROM stockprop
        WHERE type_id = (
            SELECT cvterm_id FROM cvterm WHERE name = 'pick_date'
        );

-- make them visible to views as..
-- .. relationship
INSERT INTO tripal_views_join
    (setup_id, base_table, base_field, left_table, left_field, handler)
        VALUES (
            (
                SELECT 1 + (
                    SELECT setup_id
                        FROM tripal_views_field
                        ORDER BY setup_id DESC
                        LIMIT 1
                )
            ), 'stock_date_pick', 'stock_id', 'stock', 'stock_id',
            'views_handler_join');
INSERT INTO tripal_views_join
    (setup_id, base_table, base_field, left_table, left_field, handler)
        VALUES (
            (
                SELECT 2 + (
                    SELECT setup_id
                        FROM tripal_views_field
                        ORDER BY setup_id DESC
                        LIMIT 1
                )
            ), 'stock_date_plant', 'stock_id', 'stock', 'stock_id',
            'views_handler_join');
-- .. fields
-- TODO make v numbers variables or smth.
INSERT INTO tripal_views_field
    VALUES (321, 'plant_date', 'Plant Date', 'Stock Plant Date', 'varchar');
INSERT INTO tripal_views_field
    VALUES (321, 'stock_id', 'Stock ID', 'Stock Stock ID', 'varchar');

INSERT INTO tripal_views_field
    VALUES (322, 'pick_date', 'Pick Date', 'Stock Pick Date', 'varchar');
INSERT INTO tripal_views_field
    VALUES (322, 'stock_id', 'Stock ID', 'Stock Stock ID', 'varchar');

INSERT INTO tripal_views_join
    (setup_id, base_table, base_field, left_table, left_field, handler, relationship_only)
    VALUES (
        (SELECT setup_id FROM tripal_views_join WHERE base_table='stock' limit 1),
        'stock', 'stock_id', 'stock_date_plant', 'stock_id', 'views_join', 1
    );
INSERT INTO tripal_views_join
    (setup_id, base_table, base_field, left_table, left_field, handler, relationship_only)
    VALUES (
        (SELECT setup_id FROM tripal_views_join WHERE base_table='stock' limit 1),
        'stock', 'stock_id', 'stock_date_pick', 'stock_id', 'views_join', 1
    );
