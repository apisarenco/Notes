import time, psycopg2, sys

conn = psycopg2.connect('dbname=test')
cur = conn.cursor()

def recreate_schema():
    cur.execute('DROP SCHEMA IF EXISTS pg10test CASCADE;')
    cur.execute('CREATE SCHEMA pg10test;')

def create_s(num_rows):
    cur.execute('CREATE TABLE pg10test.table_s (id INTEGER, value INTEGER, part SMALLINT);')
    cur.execute(f'INSERT INTO pg10test.table_s SELECT v, s, v % 20 FROM generate_series(1, {num_rows}, 1) s CROSS JOIN LATERAL (SELECT (random()*{num_rows})::INTEGER v) cte')

def create_p(num_rows):
    cur.execute('CREATE TABLE pg10test.table_p (id INTEGER, value INTEGER, part SMALLINT) PARTITION BY LIST (part);')
    for i in range(0, 20):
        cur.execute(f'CREATE TABLE pg10test.table_p_{i} PARTITION OF pg10test.table_p FOR VALUES IN ({i});')
    cur.execute(f'INSERT INTO pg10test.table_p SELECT v, s, v % 20 FROM generate_series(1, {num_rows}, 1) s CROSS JOIN LATERAL (SELECT (random()*{num_rows})::INTEGER v) cte')

def index_s():
    cur.execute(f'CREATE INDEX table_s_idx ON pg10test.table_s (id) WITH (fillfactor=100);')

def index_p():
    for i in range(0, 20):
        cur.execute(f'CREATE INDEX table_p_{i}_idx ON pg10test.table_p_{i} (id) WITH (fillfactor=100);')

def create_s1(num_rows):
    cur.execute(f'CREATE TABLE pg10test.table_s1 (id INTEGER, part SMALLINT);')
    cur.execute(f'INSERT INTO pg10test.table_s1 SELECT s, s % 20 FROM generate_series(1, {num_rows} / 10, 1) s ORDER BY random()')

def create_p1(num_rows):
    cur.execute(f'CREATE TABLE pg10test.table_p1 (id INTEGER, part SMALLINT) PARTITION BY LIST (part);')
    for i in range(0, 20):
        cur.execute(f'CREATE TABLE pg10test.table_p1_{i} PARTITION OF pg10test.table_p1 FOR VALUES IN ({i});')
    cur.execute(f'INSERT INTO pg10test.table_p1 SELECT s, s % 20 FROM generate_series(1, {num_rows} / 10, 1) s ORDER BY random()')

def join_s():
    cur.execute('SELECT avg(value) FROM pg10test.table_s1 LEFT JOIN pg10test.table_s USING (id);')
    cur.fetchall()

def join_p():
    for i in range(0, 20):
        cur.execute(f'SELECT avg(value) FROM pg10test.table_p1 p1 LEFT JOIN pg10test.table_p p ON p.id=p1.id AND p.part={i} WHERE p1.part={i};')
        cur.fetchall()

timers = {
    "s": {
        "insert": {},
        "index": {},
        "join": {}
    },
    "p": {
        "insert": {},
        "index": {},
        "join": {}
    }}
for i in range(3, 9):
    num_rows = 10**i
    recreate_schema()

    start = time.time()
    create_s(num_rows)
    create_s1(num_rows)
    elapsed = (time.time()-start)
    timers["s"]["insert"][i]=elapsed
    print(f'single insert for 10^{i} rows = {elapsed}s')
    sys.stdout.flush()

    start = time.time()
    create_p(num_rows)
    create_p1(num_rows)
    elapsed = (time.time()-start)
    timers["p"]["insert"][i]=elapsed
    print(f'partitioned insert for 10^{i} rows = {elapsed}s')
    sys.stdout.flush()

    start = time.time()
    index_s()
    elapsed = (time.time()-start)
    timers["s"]["index"][i]=elapsed
    print(f'single index for 10^{i} rows = {elapsed}s')
    sys.stdout.flush()

    start = time.time()
    index_p()
    elapsed = (time.time()-start)
    timers["p"]["index"][i]=elapsed
    print(f'partitioned index for 10^{i} rows = {elapsed}s')
    sys.stdout.flush()

    start = time.time()
    join_s()
    elapsed = (time.time()-start)
    timers["p"]["join"][i]=elapsed
    print(f'single join for 10^{i} rows = {elapsed}s')
    sys.stdout.flush()

    start = time.time()
    join_p()
    elapsed = (time.time()-start)
    timers["p"]["join"][i]=elapsed
    print(f'partitioned join for 10^{i} rows = {elapsed}s')
    sys.stdout.flush()

print(timers)
