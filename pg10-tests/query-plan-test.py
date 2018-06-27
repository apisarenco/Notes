# For this, we create a wide, large table with events
# Then join it with a day dimension table
# And then a couple of other small tables

# Do it in 3 ways:
# * Non-partitioned
# * Partition big table by chunk, then month
# * Partition big table and day dimension by month (and chunk for big table)
# Always create foreign keys. Add check constraints to try speed things up

# Check query plans and execution time for N(big table)=100M with 500 distinct days (2M per day), N(day)=3000

import time, psycopg2, sys

conn = psycopg2.connect('dbname=dwh_etl_dev')
cur = conn.cursor()

cur.execute('DROP SCHEMA IF EXISTS pg10test CASCADE;')
cur.execute('CREATE SCHEMA pg10test;')

sys.stdout.write("Creating small tables\n")
sys.stdout.flush()

cur.execute('''
CREATE TABLE pg10test.days (
    day_id INTEGER PRIMARY KEY,
    month_id INTEGER,
    day_date DATE
);

INSERT INTO pg10test.days
SELECT
    to_char(d, 'YYYYMMDD')::INTEGER AS day_id,
    to_char(d, 'YYYYMM')::INTEGER,
    d::DATE
FROM generate_series(CURRENT_DATE - INTERVAL '3000 days', CURRENT_DATE, INTERVAL '1 day') d;
''')

for n in range(1, 3):
    cur.execute(f'''
    CREATE TABLE pg10test.next{n} (
        id INTEGER PRIMARY KEY,
        val TEXT
    );

    INSERT INTO pg10test.next{n}
    SELECT
        v,
        md5(v || 'val{n}')
    FROM generate_series(1, 10, 1) v;
    ''')

sys.stdout.write("Creating big table data\n")
sys.stdout.flush()

cur.execute('''CREATE TABLE pg10test.events (
    id SERIAL,
    day_fk INTEGER,
    next1_fk INTEGER,
    next2_fk INTEGER,
    t1 TEXT,
    t2 TEXT,
    t3 TEXT,
    chunk_id SMALLINT,
    FOREIGN KEY (day_fk) REFERENCES pg10test.days (day_id),
    FOREIGN KEY (next1_fk) REFERENCES pg10test.next1 (id),
    FOREIGN KEY (next2_fk) REFERENCES pg10test.next2 (id)
);

INSERT INTO pg10test.events (
    day_fk, next1_fk, next2_fk, t1, t2, t3, chunk_id
)
SELECT
    to_char(d, 'YYYYMMDD')::INTEGER,
    n1,
    n2,
    md5(t1::TEXT || t2::TEXT || 't1::TEXT'),
    md5(t2::TEXT || t3::TEXT || 't2::TEXT'),
    md5(t3::TEXT || t1::TEXT || 't3::TEXT'),
    c::SMALLINT
FROM generate_series(CURRENT_DATE - INTERVAL '500 days', CURRENT_DATE, INTERVAL '1 day') d
CROSS JOIN generate_series(1, 10, 1) n1
CROSS JOIN generate_series(1, 10, 1) n2
CROSS JOIN generate_series(1, 10, 1) t1
CROSS JOIN generate_series(1, 10, 1) t2
CROSS JOIN generate_series(1, 10, 1) t3
CROSS JOIN generate_series(1, 2, 1) c
''')

def crbigtable(which):
    sys.stdout.write(f"Creating partitioned big table {which}\n")
    sys.stdout.flush()

    cur.execute(f'''
    CREATE TABLE pg10test.eventsp1 (
        LIKE pg10test.events
    ) PARTITION BY LIST (chunk_id);
    ''')

    for n in range(1, 3):
        cur.execute(f'''
    CREATE TABLE pg10test.eventsp{which}_{n}
    PARTITION OF pg10test.eventsp1 FOR VALUES IN (1)
    PARTITION BY RANGE (day_fk);

    DO $$
    DECLARE
        v_month_id INTEGER;
        v_min_day INTEGER;
        v_max_day INTEGER;
    BEGIN
        FOR v_month_id, v_min_day, v_max_day IN
            SELECT month_id, min(day_id), max(day_id) FROM pg10test.days GROUP BY month_id
        LOOP
            EXECUTE '
                CREATE TABLE pg10test.eventsp{which}_{n}_' || v_month_id || '
                PARTITION OF pg10test.eventsp{which}_{n} FOR VALUES FROM (' || v_min_day || ') TO (' || (v_max_day + 1) || ');
            ';
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    ''')

    cur.execute(f'''
    INSERT INTO pg10test.eventsp{which}
    SELECT * FROM pg10test.events;
    ''')


crbigtable(1)

sys.stdout.write("Constraining partitioned huge table 1\n")
sys.stdout.flush()


for n in range(1, 3):
    cur.execute(f'''
    DO $$
    DECLARE
        v_month_id INTEGER;
    BEGIN
        FOR v_month_id IN SELECT DISTINCT month_id FROM pg10test.days
        LOOP
            EXECUTE '
                ALTER TABLE TABLE pg10test.eventsp1_{n}_' || v_month_id || '
                ADD FOREIGN KEY (day_fk) REFERENCES pg10test.days (day_id);

                ALTER TABLE TABLE pg10test.eventsp1_{n}_' || v_month_id || '
                ADD FOREIGN KEY (next1_fk) REFERENCES pg10test.next1 (id);

                ALTER TABLE TABLE pg10test.eventsp1_{n}_' || v_month_id || '
                ADD FOREIGN KEY (next2_fk) REFERENCES pg10test.next1 (id);
            ';
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    ''')


crbigtable(2)


sys.stdout.write("Creating partitioned day table \n")
sys.stdout.flush()

cur.execute('''
CREATE TABLE pg10test.daysp (
    LIKE pg10test.days
) PARTITION BY LIST (month_id);
''')

cur.execute(f'''
DO $$
DECLARE
    v_month_id INTEGER;
    v_min_day INTEGER;
    v_max_day INTEGER;
BEGIN
    FOR v_month_id, v_min_day, v_max_day, v_min_date IN
        SELECT month_id, min(day_id), max(day_id) FROM pg10test.days GROUP BY month_id
    LOOP
        EXECUTE '
            CREATE TABLE pg10test.daysp_' || v_month_id || '
            PARTITION OF pg10test.daysp FOR VALUES IN (' || v_month_id || ');

            ALTER TABLE pg10test.daysp_' || v_month_id || '
            ADD CHECK (day_id BETWEEN ' || v_min_day || ' AND ' || v_max_day || ');
        ';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
''')


sys.stdout.write("Constraining partitioned huge table 2\n")
sys.stdout.flush()


for n in range(1, 3):
    cur.execute(f'''
    DO $$
    DECLARE
        v_month_id INTEGER;
    BEGIN
        FOR SELECT DISTINCT month_id FROM pg10test.days
        LOOP
            EXECUTE '
                ALTER TABLE TABLE pg10test.eventsp2_{n}_' || v_month_id || '
                ADD FOREIGN KEY (day_fk) REFERENCES pg10test.daysp_' || v_month_id || ' (day_id);

                ALTER TABLE TABLE pg10test.eventsp2_{n}_' || v_month_id || '
                ADD FOREIGN KEY (next1_fk) REFERENCES pg10test.next1 (id);

                ALTER TABLE TABLE pg10test.eventsp2_{n}_' || v_month_id || '
                ADD FOREIGN KEY (next2_fk) REFERENCES pg10test.next1 (id);
            ';
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    ''')
