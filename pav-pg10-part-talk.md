# Postgresql 10 and partitioning

## Partitioning fact tables by month range
```sql
DROP TABLE IF EXISTS os_dim.order_items_fact_p1 CASCADE;

CREATE TABLE os_dim.order_items_fact_p1 (
  LIKE os_dim.order_items_fact,
  invoice_is_null SMALLINT
) PARTITION BY LIST (invoice_is_null);

CREATE TABLE os_dim.order_items_fact_p10 PARTITION OF os_dim.order_items_fact_p1 FOR VALUES IN (0) PARTITION BY RANGE (invoice_day_fk);
CREATE TABLE os_dim.order_items_fact_p11 PARTITION OF os_dim.order_items_fact_p1 FOR VALUES IN (1);

DO $f$
DECLARE
  m INTEGER;
BEGIN
  FOR m IN SELECT DISTINCT day_id/100 FROM os_dim."day" LOOP
    EXECUTE $$
      CREATE TABLE os_dim.order_items_fact_p10_$$ || m || $$ PARTITION OF os_dim.order_items_fact_p10 FOR VALUES FROM ($$ || m*100 || $$) TO ($$ || (m+1)*100 || $$);
    $$;
  END LOOP;
END;
$f$ LANGUAGE plpgsql;

INSERT INTO os_dim.order_items_fact_p1
SELECT *, (invoice_day_fk IS NULL) :: INTEGER :: SMALLINT FROM os_dim.order_items_fact;

SELECT util.add_index('os_dim', 'order_items_fact_p1', column_names:=ARRAY['invoice_day_fk']);
SELECT util.add_index('os_dim', 'order_items_fact_p1', column_names:=ARRAY['order_day_fk']);

--0.298s
EXPLAIN (ANALYZE, VERBOSE)
SELECT count(1) FROM os_dim.order_items_fact_p1 WHERE invoice_day_fk BETWEEN 20170415 AND 20170515;

--0.343s
EXPLAIN (ANALYZE, VERBOSE)
SELECT count(1) FROM os_dim.order_items_fact_p1 WHERE order_day_fk BETWEEN 20170415 AND 20170515;

-- Typical Saiku joins and filters
EXPLAIN (ANALYZE, VERBOSE)
SELECT LENGTH(array_agg(order_id)::TEXT), sum(net_revenue) FROM os_dim.order_items_fact_p1 o
JOIN os_dim."day" d ON o.order_day_fk = d.day_id
WHERE
  TRUE
  AND order_day_fk BETWEEN 20170515 AND 20170615
  AND d.day_id BETWEEN 20170515 AND 20170615
```

Benefits:
* Blazing fast filters based on values on which it's partitioned
* Smaller, faster indexes, due to fewer levels in the btree and shorter cuts
* Measures correlated to partition values also benefit from fast filtering, due to early cutting on btree indexes
* Go from 4 minutes query to 2 seconds or fewer

Drawbacks:
* Cluttered GUI with tons of tables
* Slightly more code to write
* Particularities and limitations when using constraints
* Theoretically slower query planner for every other query, because of more tables to process

## Partitioning and parallelizing
1x
```sql
SELECT
  ...
FROM table_a
JOIN table_b
  ON table_a.b_fk = table_b.b_id
```
is far worse than:
15x
```sql
SELECT
  ...
FROM table_a
JOIN table_b
  ON table_a.b_fk = table_b.b_id
     AND table_b.chunk_id = :chunk_id
WHERE table_a.chunk_id = :chunk_id
```

Complexity in best case scenario is `O(n*log(n))`.
`15 * n/15 * log(n/15)` is much less than `n * log(n)`

### Challenges
* Grouping, window functions and partition isolation.
  * Make sure that everything you need for the computation is in the partitions you're working with.
  * Make sure that if you don't group on a field directly correlated with the partition criteria, you aggregate afterwards on the entire table if possible (`COUNT(DISTINCT x)` won't work)
* Finding and using a common partitioning scheme for most related jobs

## Partitioning as a fast index on low cardinality fields
### Situations
* Touchpoint (Session) path segment: To First Order, To Recurring Order, After Last Order, Non-Converting. All have different processing logic and are included in different computations. Speed it up by putting in different partitions.
* Incremental loading from different sources into the same structure. Add unique constraints for each partition and upsert data from each source with its own logic for primary key values. Read from the entire table at once.

## Partitioning to leverage parallel IO on multiple disks
Create your own infrastructure solutions by sticking new storage mediums and using them in parallel with `Tablespaces`.

## Partitioning is easy
#### Before
```sql
CREATE TABLE base (
.....
key_column SMALLINT
);
CREATE TABLE part_0 (
  CHECK (key_column = 0)
) INHERITS base;

CREATE OR REPLACE FUNCTION parallel_insert(partition_id SMALLINT)
RETURNS VOID AS $$
BEGIN
  EXECUTE '
    INSERT INTO part_'|| partition_id ||'
    ....;
  ';
END;
$$ LANGUAGE plpgsql;
```
* Ugly `EXECUTE` blocks
* Must make sure to insert only fields belonging to this partition, which might involve reading indexes
* Re-chunking is slow and painful (each process reads from all origin partitions to write to one destination partition)

#### After
```sql
CREATE TABLE base (
.....
key_column SMALLINT
) PARTITION BY LIST (key_column);

CREATE TABLE part_0
PARTITION OF base
FOR VALUES IN (0);

CREATE OR REPLACE FUNCTION parallel_insert(partition_id SMALLINT)
RETURNS VOID AS $$
BEGIN
  INSERT INTO base
  ....;
END;
$$ LANGUAGE plpgsql;
```
* Clean code
* No penalty for redirecting to partitions (trust me, I checked, in parallel too). Zero. None.
* Re-chunking is a breeze. Read each source data partition in its entirety, isolated, insert into the base table without a care in the world. You get a new partition scheme. With zero cost.
* You can still add checks on partitions, for additional speed-ups.

##### Still not perfect
* Need to create partitions before inserting data into them (not created automatically)
  * But you have control on how to create them, and you can partition them further, without any restrictions other than the usual partitioning restrictions.
* You can't (yet) partition on boolean values
  * Might already be fixed in a minor version. If not, PG11 will have it fixed.
* You can't create a partition for null values
  * Because it only accepts equality or range operations. And `NULL` screws them all up.
* You can't (yet) partition on expressions. Value has to come in verbatum.
  * Promised to appear in PG11.
* You can't partition on more than 1 column. Instead you must create partitions of a partition. And sometimes (I suspect) the planner sucks when it has such structures
* `ANALYZE`, adding indexes, constraints and others, can only be done on the partitions. They have no effect, or cannot even be added (constraitns) on base tables.
* Declarative partitioning cannot route records to FOREIGN tables (CStore), and underlying checks don't work.
  * But the planner does take the partitions into account so it actually can make things faster even with cstore.
* The number of partitions is limited by the OS. Queries that require reading too many partitions will [throw SQL Error](https://doxygen.postgresql.org/fd_8c.html#a1b83596398165190a393307951b8d70d) `53000` (INSUFFICIENT_RESOURCES), with the message `exceeded maxAllocatedDescs (%d) while trying to open file "%s"`.
