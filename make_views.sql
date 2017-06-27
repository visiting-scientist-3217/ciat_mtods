CREATE OR REPLACE FUNCTION makeTripalVisible ()
RETURNS integer AS $$
declare
    base_setup_id integer;
    next_setup_id integer;
    next_setup_id integer;
BEGIN
    -- create views
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

    -- get setup_ids
    SELECT setup_id into base_setup_id
        FROM tripal_views_field
        ORDER BY setup_id DESC
        LIMIT 1
    SELECT (1 + base_setup_id) into next_setup_id;
    SELECT (2 + base_setup_id) into nextnext_setup_id;

    -- link all the things, chado style
    INSERT INTO tripal_views_join
        (setup_id, base_table, base_field, left_table, left_field, handler)
        VALUES (
            next_setup_id, 'stock_date_pick', 'stock_id',
            'stock', 'stock_id', 'views_handler_join'
        );
    INSERT INTO tripal_views_join
        (setup_id, base_table, base_field, left_table, left_field, handler)
        VALUES (
            nextnext_setup_id, 'stock_date_plant', 'stock_id',
            'stock', 'stock_id', 'views_handler_join'
        );
    INSERT INTO tripal_views_field
        VALUES (next_setup_id, 'plant_date', 'Plant Date', 'Stock
                Plant Date', 'varchar');
    INSERT INTO tripal_views_field
        VALUES (next_setup_id, 'stock_id', 'Stock ID', 'Stock
                Stock ID', 'varchar');
    INSERT INTO tripal_views_field
        VALUES (nextnext_setup_id, 'pick_date', 'Pick Date', 'Stock
                Pick Date', 'varchar');
    INSERT INTO tripal_views_field
        VALUES (nextnext_setup_id, 'stock_id', 'Stock ID', 'Stock
                Stock ID', 'varchar');

    INSERT INTO tripal_views_join
        (setup_id, base_table, base_field, left_table, left_field, handler,
         relationship_only)
        VALUES (
            (SELECT setup_id FROM tripal_views_join WHERE base_table='stock'
             limit 1),
            'stock', 'stock_id', 'stock_date_plant', 'stock_id', 'views_join', 1
        );
    INSERT INTO tripal_views_join
        (setup_id, base_table, base_field, left_table, left_field, handler,
         relationship_only)
        VALUES (
            (SELECT setup_id FROM tripal_views_join WHERE base_table='stock'
             limit 1),
            'stock', 'stock_id', 'stock_date_pick', 'stock_id', 'views_join', 1
        );
    RETURN next_setup_id;
END;
$$ LANGUAGE plpgsql;

