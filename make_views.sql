----
-- Note:
--  After execution, one must run the drupal7 cron job, otherwise the enabled
--  view's might not show up.
----
-- TODO use tripal_mviews instead of tripal_views!
----
CREATE OR REPLACE FUNCTION makeDatesTripalVisible ()
RETURNS integer AS $$
declare
    next_setup_id integer;
    nextnext_setup_id integer;
    stock_setup_id integer;
BEGIN
    -- create views
    CREATE VIEW stock_date_plant (stock_id, plant_date) AS
        SELECT stock_id, value
            FROM stockprop
            WHERE type_id = (
                SELECT cvterm_id FROM cvterm WHERE name = 'plant_date'
            );

    CREATE VIEW stock_date_pick (stock_id, pick_date) AS
        SELECT stock_id, value
            FROM stockprop
            WHERE type_id = (
                SELECT cvterm_id FROM cvterm WHERE name = 'pick_date'
            );

    -- tell tripal that these tables exist, and get the setup_ids
    INSERT INTO tripal_views
            (table_name, name) VALUES ('stock_date_plant', 'Plant Date');
    SELECT setup_id INTO next_setup_id FROM tripal_views
        WHERE table_name = 'stock_date_plant' AND name = 'Plant Date';
    INSERT INTO tripal_views
            (table_name, name) VALUES ('stock_date_pick', 'Pick Date');
    SELECT setup_id INTO nextnext_setup_id FROM tripal_views
        WHERE table_name = 'stock_date_pick' AND name = 'Pick Date';
    SELECT setup_id INTO stock_setup_id
        FROM tripal_views_join WHERE base_table = 'stock' LIMIT 1;

    -- link it, tripal style
    INSERT INTO tripal_views_join
            (setup_id, base_table, base_field, left_table, left_field, handler)
        VALUES
            (next_setup_id, 'stock_date_plant', 'stock_id', 'stock',
             'stock_id', 'views_handler_join'),
            (nextnext_setup_id, 'stock_date_pick', 'stock_id', 'stock',
             'stock_id', 'views_handler_join');
    INSERT INTO tripal_views_join
            (setup_id, base_table, base_field, left_table, left_field, handler,
             relationship_only)
        VALUES
            (stock_setup_id, 'stock', 'stock_id', 'stock_date_plant',
             'stock_id', 'views_join', 1),
            (stock_setup_id, 'stock', 'stock_id', 'stock_date_pick',
             'stock_id', 'views_join', 1);

    -- make the fields accessible
    -- Note: This fails sometimes for no obvious reason, and a 2nd execution
    --       just works fine.
    -- TODO: find out what happened.
    INSERT INTO tripal_views_field
        VALUES
            (next_setup_id, 'plant_date', 'Plant Date', 'Stock Plant Date',
             'varchar'),
            (next_setup_id, 'stock_id', 'Stock ID', 'Stock Stock ID',
             'varchar'),
            (nextnext_setup_id, 'pick_date', 'Pick Date', 'Stock Pick Date',
             'varchar'),
            (nextnext_setup_id, 'stock_id', 'Stock ID', 'Stock Stock ID',
             'varchar');

    -- select default handlers for field/filter/sort
    INSERT INTO tripal_views_handlers
            (setup_id, column_name, handler_type, handler_name, arguments)
        VALUES
            (next_setup_id, 'stock_id', 'field', 'views_handler_field_numeric', 'a:1:{s:4:"name";s:27:"views_handler_field_numeric";}'),
            (next_setup_id, 'stock_id', 'filter', 'views_handler_filter_numeric', 'a:1:{s:4:"name";s:28:"views_handler_filter_numeric";}'),
            (next_setup_id, 'stock_id', 'sort', 'views_handler_sort', 'a:1:{s:4:"name";s:18:"views_handler_sort";}'),
            (next_setup_id, 'stock_id', 'argument', 'views_handler_argument_numeric', 'a:1:{s:4:"name";s:30:"views_handler_argument_numeric";}'),
            (nextnext_setup_id, 'stock_id', 'field', 'views_handler_field_numeric', 'a:1:{s:4:"name";s:27:"views_handler_field_numeric";}'),
            (nextnext_setup_id, 'stock_id', 'filter', 'views_handler_filter_numeric', 'a:1:{s:4:"name";s:28:"views_handler_filter_numeric";}'),
            (nextnext_setup_id, 'stock_id', 'sort', 'views_handler_sort', 'a:1:{s:4:"name";s:18:"views_handler_sort";}'),
            (nextnext_setup_id, 'stock_id', 'argument', 'views_handler_argument_numeric', 'a:1:{s:4:"name";s:30:"views_handler_argument_numeric";}'),
            (next_setup_id, 'plant_date', 'field', 'views_handler_field', 'a:1:{s:4:"name";s:19:"views_handler_field";}'),
            (next_setup_id, 'plant_date', 'filter', 'views_handler_filter_string', 'a:1:{s:4:"name";s:27:"views_handler_filter_string";}'),
            (next_setup_id, 'plant_date', 'sort', 'views_handler_sort', 'a:1:{s:4:"name";s:18:"views_handler_sort";}'),
            (next_setup_id, 'plant_date', 'argument', 'views_handler_argument_string', 'a:1:{s:4:"name";s:29:"views_handler_argument_string";}'),
            (nextnext_setup_id, 'pick_date', 'field', 'views_handler_field', 'a:1:{s:4:"name";s:19:"views_handler_field";}'),
            (nextnext_setup_id, 'pick_date', 'filter', 'views_handler_filter_string', 'a:1:{s:4:"name";s:27:"views_handler_filter_string";}'),
            (nextnext_setup_id, 'pick_date', 'sort', 'views_handler_sort', 'a:1:{s:4:"name";s:18:"views_handler_sort";}'),
            (nextnext_setup_id, 'pick_date', 'argument', 'views_handler_argument_string', 'a:1:{s:4:"name";s:29:"views_handler_argument_string";}');
    RETURN next_setup_id;
END;
$$ LANGUAGE plpgsql;

-- Undo everything.
CREATE OR REPLACE FUNCTION makeDatesTripalInvisible ()
RETURNS integer AS $$
declare
    next_setup_id integer;
    nextnext_setup_id integer;
BEGIN
    DROP VIEW stock_date_pick;
    DROP VIEW stock_date_plant;
    SELECT setup_id INTO next_setup_id FROM tripal_views
        WHERE table_name = 'stock_date_plant' AND name = 'Plant Date';
    SELECT setup_id INTO nextnext_setup_id FROM tripal_views
        WHERE table_name = 'stock_date_pick' AND name = 'Pick Date';
    DELETE FROM tripal_views_join
        WHERE base_table LIKE 'stock_date_p%'
            OR left_table LIKE 'stock_date_p%'
            OR setup_id = ANY(ARRAY[next_setup_id, nextnext_setup_id]);
    DELETE FROM tripal_views_field
        WHERE column_name = ANY(ARRAY['plant_date', 'pick_date'])
            OR setup_id = ANY(ARRAY[next_setup_id, nextnext_setup_id]);
    DELETE FROM tripal_views
        WHERE table_name = ANY(ARRAY['plant_date', 'pick_date'])
            OR setup_id = ANY(ARRAY[next_setup_id, nextnext_setup_id]);
    DELETE FROM tripal_views_handlers
        WHERE setup_id = ANY(ARRAY[next_setup_id, nextnext_setup_id]);
    RETURN 1;
END;
$$ LANGUAGE plpgsql;
