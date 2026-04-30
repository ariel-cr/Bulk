CREATE OR REPLACE TRIGGER TRG_PROCESS_CDC_INBOX
FOR INSERT ON CDC_INBOX
COMPOUND TRIGGER

    TYPE t_id_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    TYPE t_str_arr IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;
    TYPE t_clob_arr IS TABLE OF CLOB INDEX BY PLS_INTEGER;

    g_ids       t_id_arr;
    g_types     t_str_arr;
    g_events    t_str_arr;
    g_payloads  t_clob_arr;
    g_idx       PLS_INTEGER := 0;

    BEFORE STATEMENT IS
    BEGIN
        g_ids.DELETE; g_types.DELETE; g_events.DELETE; g_payloads.DELETE;
        g_idx := 0;
    END BEFORE STATEMENT;

    AFTER EACH ROW IS
    BEGIN
        g_idx := g_idx + 1;
        g_ids(g_idx) := :NEW.ID;
        g_types(g_idx) := :NEW.AGGREGATE_TYPE;
        g_events(g_idx) := :NEW.EVENT_TYPE;
        g_payloads(g_idx) := :NEW.PAYLOAD;
    END AFTER EACH ROW;

    AFTER STATEMENT IS
        v_sp  VARCHAR2(300);
        v_err VARCHAR2(4000);
    BEGIN
        FOR i IN 1 .. g_idx LOOP
            BEGIN
                v_sp := NULL;
                BEGIN
                    SELECT SP_NAME INTO v_sp FROM CDC_INBOX_MODULE_CONFIG
                    WHERE AGGREGATE_TYPE = g_types(i) AND ACTIVE = 1;
                EXCEPTION WHEN NO_DATA_FOUND THEN v_sp := NULL; END;

                IF v_sp IS NOT NULL THEN
                    EXECUTE IMMEDIATE 'BEGIN '||v_sp||'(:1, :2, :3, :4); END;'
                        USING g_ids(i), g_types(i), g_events(i), g_payloads(i);

                    UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP
                    WHERE ID = g_ids(i);
                END IF;
            EXCEPTION WHEN OTHERS THEN
                v_err := SQLERRM;
                INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
                VALUES (g_ids(i), g_types(i), g_events(i), SUBSTR(v_err,1,4000));
            END;
        END LOOP;
    END AFTER STATEMENT;

END TRG_PROCESS_CDC_INBOX;
