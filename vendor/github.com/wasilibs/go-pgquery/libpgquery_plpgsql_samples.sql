-- Examples from https://www.postgresql.org/docs/9.6/static/plpgsql-control-structures.html

CREATE OR REPLACE FUNCTION get_all_foo() RETURNS SETOF foo AS
$BODY$
DECLARE
    r foo%rowtype;
BEGIN
    FOR r IN
        SELECT * FROM foo WHERE fooid > 0
    LOOP
        -- can do some processing here
        RETURN NEXT r; -- return current row of SELECT
    END LOOP;
    RETURN;
END
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION get_available_flightid(date) RETURNS SETOF integer AS
$BODY$
BEGIN
    RETURN QUERY SELECT flightid
                   FROM flight
                  WHERE flightdate >= $1
                    AND flightdate < ($1 + 1);

    -- Since execution is not finished, we can check whether rows were returned
    -- and raise exception if not.
    IF NOT FOUND THEN
        RAISE EXCEPTION 'No flight at %.', $1;
    END IF;

    RETURN;
 END
$BODY$
LANGUAGE plpgsql;

-- Examples from https://www.postgresql.org/docs/9.6/static/plpgsql-porting.html

CREATE OR REPLACE FUNCTION cs_fmt_browser_version(v_name varchar,
                                                  v_version varchar)
RETURNS varchar AS $$
BEGIN
  IF v_version IS NULL THEN
    RETURN v_name;
  END IF;
  RETURN v_name || '/' || v_version;
END;$$;

-- CREATE OR REPLACE FUNCTION cs_update_referrer_type_proc() RETURNS void AS $func$
-- DECLARE
--     referrer_keys CURSOR IS
--         SELECT * FROM cs_referrer_keys
--         ORDER BY try_order;
--     func_body text;
--     func_cmd text;
-- BEGIN
--   func_body := 'BEGIN';
--   -- Notice how we scan through the results of a query in a FOR loop
--   -- using the FOR <record> construct.
--   FOR referrer_key IN SELECT * FROM cs_referrer_keys ORDER BY try_order LOOP
--       func_body := func_body ||
--         ' IF v_' || referrer_key.kind
--         || ' LIKE ' || quote_literal(referrer_key.key_string)
--         || ' THEN RETURN ' || quote_literal(referrer_key.referrer_type)
--         || '; END IF;' ;
--   END LOOP;
--   func_body := func_body || ' RETURN NULL; END;';
--   func_cmd :=
--     'CREATE OR REPLACE FUNCTION cs_find_referrer_type(v_host varchar,
--                                                       v_domain varchar,
--                                                       v_url varchar)
--       RETURNS varchar AS '
--     || quote_literal(func_body)
--     || ' LANGUAGE plpgsql;' ;
--   EXECUTE func_cmd;
-- END;$func$;

CREATE OR REPLACE FUNCTION cs_parse_url(
    v_url IN VARCHAR,
    v_host OUT VARCHAR,  -- This will be passed back
    v_path OUT VARCHAR,  -- This one too
    v_query OUT VARCHAR) -- And this one
AS $$
DECLARE
    a_pos1 INTEGER;
    a_pos2 INTEGER;
BEGIN
    v_host := NULL;
    v_path := NULL;
    v_query := NULL;
    a_pos1 := instr(v_url, '//');

    IF a_pos1 = 0 THEN
        RETURN;
    END IF;
    a_pos2 := instr(v_url, '/', a_pos1 + 2);
    IF a_pos2 = 0 THEN
        v_host := substr(v_url, a_pos1 + 2);
        v_path := '/';
        RETURN;
    END IF;

    v_host := substr(v_url, a_pos1 + 2, a_pos2 - a_pos1 - 2);
    a_pos1 := instr(v_url, '?', a_pos2 + 1);

    IF a_pos1 = 0 THEN
        v_path := substr(v_url, a_pos2);
        RETURN;
    END IF;

    v_path := substr(v_url, a_pos2, a_pos1 - a_pos2);
    v_query := substr(v_url, a_pos1 + 1);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cs_create_job(v_job_id integer) RETURNS void AS $$
DECLARE
    a_running_job_count integer;
BEGIN
    LOCK TABLE cs_jobs IN EXCLUSIVE MODE;

    SELECT count(*) INTO a_running_job_count FROM cs_jobs WHERE end_stamp IS NULL;

    IF a_running_job_count > 0 THEN
        RAISE EXCEPTION 'Unable to create a new job: a job is currently running';
    END IF;

    DELETE FROM cs_active_job;
    INSERT INTO cs_active_job(job_id) VALUES (v_job_id);

    BEGIN
        INSERT INTO cs_jobs (job_id, start_stamp) VALUES (v_job_id, now());
    EXCEPTION
        WHEN unique_violation THEN
            -- don't worry if it already exists
    END;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION instr(varchar, varchar) RETURNS integer AS $$
DECLARE
    pos integer;
BEGIN
    pos:= instr($1, $2, 1);
    RETURN pos;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE FUNCTION instr(string varchar, string_to_search varchar, beg_index integer)
RETURNS integer AS $$
DECLARE
    pos integer NOT NULL DEFAULT 0;
    temp_str varchar;
    beg integer;
    length integer;
    ss_length integer;
BEGIN
    IF beg_index > 0 THEN
        temp_str := substring(string FROM beg_index);
        pos := position(string_to_search IN temp_str);

        IF pos = 0 THEN
            RETURN 0;
        ELSE
            RETURN pos + beg_index - 1;
        END IF;
    ELSIF beg_index < 0 THEN
        ss_length := char_length(string_to_search);
        length := char_length(string);
        beg := length + beg_index - ss_length + 2;

        WHILE beg > 0 LOOP
            temp_str := substring(string FROM beg FOR ss_length);
            pos := position(string_to_search IN temp_str);

            IF pos > 0 THEN
                RETURN beg;
            END IF;

            beg := beg - 1;
        END LOOP;

        RETURN 0;
    ELSE
        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE FUNCTION instr(string varchar, string_to_search varchar,
                      beg_index integer, occur_index integer)
RETURNS integer AS $$
DECLARE
    pos integer NOT NULL DEFAULT 0;
    occur_number integer NOT NULL DEFAULT 0;
    temp_str varchar;
    beg integer;
    i integer;
    length integer;
    ss_length integer;
BEGIN
    IF beg_index > 0 THEN
        beg := beg_index;
        temp_str := substring(string FROM beg_index);

        FOR i IN 1..occur_index LOOP
            pos := position(string_to_search IN temp_str);

            IF i = 1 THEN
                beg := beg + pos - 1;
            ELSE
                beg := beg + pos;
            END IF;

            temp_str := substring(string FROM beg + 1);
        END LOOP;

        IF pos = 0 THEN
            RETURN 0;
        ELSE
            RETURN beg;
        END IF;
    ELSIF beg_index < 0 THEN
        ss_length := char_length(string_to_search);
        length := char_length(string);
        beg := length + beg_index - ss_length + 2;

        WHILE beg > 0 LOOP
            temp_str := substring(string FROM beg FOR ss_length);
            pos := position(string_to_search IN temp_str);

            IF pos > 0 THEN
                occur_number := occur_number + 1;

                IF occur_number = occur_index THEN
                    RETURN beg;
                END IF;
            END IF;

            beg := beg - 1;
        END LOOP;

        RETURN 0;
    ELSE
        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

-- Additional test data below kindly provided by Olivier Auverlot and used with permission

CREATE FUNCTION displayDate(endDate date, canceled boolean) RETURNS text
    LANGUAGE plpgsql
    AS $$BEGIN
	IF canceled = true THEN
		return null;
	ELSE
		return endDate;
	END IF;
END;$$;

CREATE FUNCTION calcule_theYear_these(date_inscription date, date_observation date) RETURNS smallint
    LANGUAGE plpgsql
    AS $$BEGIN
	return (calcule_duree(date_inscription,date_observation) + 1);
END;$$;

CREATE FUNCTION calcule_duree(origine date, atDate date) RETURNS integer
    LANGUAGE plpgsql
    AS $$DECLARE
	theDay INTEGER;
	theMonth INTEGER;
	theYear INTEGER;
	theDay_now INTEGER;
	theMonth_now INTEGER;
	theYear_now INTEGER;
BEGIN
	theDay := EXTRACT(DAY FROM origine);
	theMonth := EXTRACT(MONTH FROM origine);
	theYear := EXTRACT(YEAR FROM origine);

	theDay_now := EXTRACT(DAY FROM atDate);
	theMonth_now := EXTRACT(MONTH FROM atDate);
	theYear_now := EXTRACT(YEAR FROM atDate);

	theYear := theYear_now - theYear;
	if theMonth_now <= theMonth THEN
		IF theMonth = theMonth_now THEN
			IF theDay > theDay_now THEN
				theYear := theYear - 1;
			END IF;
		ELSE
			theYear := theYear - 1;
		END IF;
	END IF;

	return theYear;
END;$$;

CREATE FUNCTION endDatedUID(uidmember character varying) RETURNS integer
    LANGUAGE plpgsql
    AS $$DECLARE
	memberID int4;

BEGIN
	SELECT key INTO memberID
	FROM
		member
	WHERE
		uidmember = uid;

	RETURN memberID;
END;$$;

CREATE FUNCTION currentEmployer(memberID integer, jobID integer, jobEnd date) RETURNS boolean
    LANGUAGE plpgsql
    AS $$DECLARE
	lastJob RECORD;
	lastEmployer record;
	updateJob BOOL;
BEGIN
	updateJob := false;

	SELECT * INTO lastJob FROM lire_lastJob(memberID) AS (jobID INT,startsupport DATE,jobEnd DATE);
	IF lastJob.jobID = jobID THEN
		SELECT
			r_perlab.key AS positionHeld,
			r_perlab.endDate AS positionEnd
		INTO lastEmployer
		FROM
			r_perlab,
			(SELECT
				r_perlab.key_member AS col_memberID,
				max(r_perlab.start) AS startrattachement
			FROM r_perlab
			GROUP BY col_memberID) positions
		WHERE ((positions.col_memberID = memberID) AND (r_perlab.key_member = positions.col_memberID) AND (r_perlab.start = startrattachement));

		IF lastEmployer.positionHeld IS NOT NULL THEN
			updateJob := true;
			UPDATE r_perlab SET endDate = jobEnd WHERE key = lastEmployer.positionHeld;
		END IF;
	END IF;

	RETURN updateJob;
END;$$;

-- CREATE FUNCTION getArrivalDate(memberID integer, teamID integer, atDate date) RETURNS date
--     LANGUAGE plpgsql
--     AS $$
-- DECLARE
--         affectations affectation[];
--         aff affectation;
--
--         DELAI_MAX CONSTANT INTEGER := 30;
--
--         idx INTEGER;
-- 		nbr_aff INTEGER;
--         start DATE;
--         endDate DATE;
--         sortie BOOLEAN;
--
--         arrivalDate DATE;
--
--         crs_affectation CURSOR (memberID INTEGER) FOR
--                 SELECT
--                         affectation.key,
--                         affectation.key_team,
--                         affectation.key_support,
--                         affectation.start,
--                         affectation.endDate,
--                         affectation.repartition,
--                         affectation.key_typeaffectation
--                 FROM support,affectation
--                 WHERE
--                         support.key_member = memberID
--                 AND     affectation.key_support = support.key
--                 AND     affectation.start <= atDate
--                 AND     affectation.key_team = teamID
--                 ORDER BY affectation.start DESC;
-- BEGIN
--         OPEN crs_affectation(memberID);
--         LOOP
--                 IF NOT FOUND THEN
--                         EXIT;
--                 END IF;
--                 FETCH crs_affectation into aff.key,aff.key_team,aff.key_support,aff.start,aff.endDate,aff.repartition,aff.key_typeaffectation;
--                 affectations := ARRAY_APPEND(affectations,aff);
--         END LOOP;
--         CLOSE crs_affectation;
--
--         nbr_aff := ARRAY_LENGTH(affectations, 1);
-- 		idx := 1;
--         sortie := FALSE;
--
--         WHILE (idx <= nbr_aff AND sortie = FALSE) LOOP
--                 IF(arrivalDate IS NULL) THEN
--                         arrivalDate := affectations[idx].start;
--                         endDate := affectations[idx].endDate;
--                 ELSE
--                         IF(arrivalDate - affectations[idx].endDate) < DELAI_MAX THEN
--                                 arrivalDate := affectations[idx].start;
--                                 endDate := affectations[idx].endDate;
--                         ELSE
--                                 sortie := TRUE;
--                         END IF;
--                 END IF;
--                 idx := idx + 1;
--         END LOOP;
--
--         RETURN arrivalDate;
-- END;$$;

CREATE FUNCTION cleanString(str character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$

DECLARE
	spechar VARCHAR[ ] := ARRAY['à','â','é','è','ê','ë','ï','î','ô','û','ù','À','Â','É','È','Ê','Ë','Ï','Î','ô','û','ù','ç' ];
	lettres VARCHAR[ ] := ARRAY['a','a','e','e','e','e','i','i','o','u','u','a','a','e','e','e','e','i','i','o','u','u','c' ];
	resultat VARCHAR;
	nbrspechar INTEGER := 23;

BEGIN
	IF (str IS NOT NULL) THEN
		resultat := str;
		FOR i IN 1..nbrspechar LOOP
			resultat := regexp_replace(resultat,spechar[i],lettres[i],'g');
		END LOOP;
	END IF;
	RETURN resultat;
END;$$;

CREATE FUNCTION t_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$

BEGIN
        NEW.name = upper(cleanString(NEW.name));
        return NEW;
END;$$;


CREATE FUNCTION t_create() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
	INSERT INTO list(key,date) VALUES(NEW.key,NEW.end);
	RETURN NEW;
END;$$;

CREATE OR REPLACE FUNCTION test.test_parse (
    p_time_start timestamptz,
    p_time_end timestamptz,
    p_time_interval interval default NULL
) RETURNS TABLE (
    ts timestamptz,
    arbitrary_return bigint
) AS $$
BEGIN
    -- some comment
    -- some other comment

    IF p_time_interval IS NULL
        THEN p_time_interval := interval_from_start_end(p_time_start, p_time_end);
    END IF;
    RETURN QUERY
    SELECT
        bucket_function(p_time_interval, timestamp) AS ts,
        arbitrary_return
    FROM test.some_table
    WHERE
        start >= p_time_start
        AND "end" < p_time_end
    GROUP BY 1;
END; $$ LANGUAGE plpgsql SECURITY DEFINER PARALLEL UNSAFE;

CREATE FUNCTION public.somefunc(OUT _result uuid[])
    RETURNS uuid[]
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	active_on_to_date uuid[];
BEGIN
_result := ARRAY( SELECT some_id FROM some_table);
END;
$BODY$;

CREATE OR REPLACE FUNCTION cs_fmt_browser_version(v_name varchar, v_version varchar)
RETURNS varchar AS $$
DECLARE
_a int;
_v_name_alias ALIAS FOR $1;
BEGIN
IF v_version IS NULL THEN
RETURN v_name;
END IF;

RETURN v_name || '/' || v_version;
END; $$ LANGUAGE plpgsql;

CREATE FUNCTION test(str character varying) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v3 RECORD;
  v4 integer;
BEGIN
  select 1 as c1, 2 as c2 into v3;
  v3.c1 := 4;
END;$$;

CREATE FUNCTION test_assert() RETURNS integer
LANGUAGE plpgsql
AS $$
BEGIN
  ASSERT true;
  ASSERT now() < '2000-01-01';
  ASSERT false, 'msg';
  ASSERT false, version();

  RETURN 1;
END;$$;

-- Example from https://www.postgresql.org/docs/current/sql-do.html
DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT table_schema, table_name FROM information_schema.tables
             WHERE table_type = 'VIEW' AND table_schema = 'public'
    LOOP
        EXECUTE 'GRANT ALL ON ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' TO webuser';
    END LOOP;
END$$;

CREATE FUNCTION test_cursor() RETURNS void AS $$
  DECLARE
    i INT;
    c CURSOR FOR SELECT generate_series(1,10);
  BEGIN
    FOR i IN c LOOP
      RAISE NOTICE 'i is %',i;
    END LOOP;
  END
$$ language plpgsql;

CREATE FUNCTION public.dz_sumfunc(
    IN  p_in  INTEGER
    ,OUT p_out public.dz_sumthing
)
AS $BODY$
DECLARE
BEGIN
    p_out.sumattribute := p_in;
END;
$BODY$
LANGUAGE plpgsql;

-- Examples from https://www.postgresql.org/docs/16/plpgsql-declarations.html#PLPGSQL-DECLARATION-PARAMETERS
CREATE FUNCTION sales_tax(real) RETURNS real AS $$
DECLARE
    subtotal ALIAS FOR $1;
BEGIN
    RETURN subtotal * 0.06;
END;
$$ LANGUAGE plpgsql;

-- RETURN NEXT; with no expression https://www.postgresql.org/docs/16/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING-RETURN-NEXT
CREATE OR REPLACE FUNCTION public.test_pl(s integer, e integer)
  RETURNS TABLE(id INTEGER) AS $$
BEGIN
  id := s;
LOOP
  EXIT WHEN id>e;
  RETURN NEXT;
  id := id + 1;
END LOOP;
END
$$
  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trgfn()
 RETURNS trigger AS $$
DECLARE
 prior ALIAS FOR old;
 updated ALIAS FOR new;
BEGIN
 RETURN;
END;
$$
  LANGUAGE plpgsql;

-- Example from https://www.postgresql.org/docs/16/plpgsql-cursors.html
CREATE FUNCTION reffunc2() RETURNS refcursor AS '
DECLARE
    ref refcursor;
BEGIN
    OPEN ref FOR SELECT col FROM test;
    RETURN ref;
END;
' LANGUAGE plpgsql;

-- Example from https://www.postgresql.org/docs/16/plpgsql-declarations.html#PLPGSQL-DECLARATION-COLLATION
CREATE FUNCTION less_than(a text, b text) RETURNS boolean AS $$
DECLARE
    local_a text COLLATE "en_US" := a;
    local_b text := b;
BEGIN
    RETURN local_a < local_b;
END;
$$ LANGUAGE plpgsql;