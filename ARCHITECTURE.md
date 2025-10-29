Overview

The objective is to provide the AWS Server access logging feature.

This feature records requests made to a bucket. It is enabled per bucket. When enabled, the system delivers log records as objects to a destination bucket.

Requirements
This feature (at least in its first version) is not designed to be used for security auditing.
This aligns with the AWS feature. From AWS documentation: “The purpose of server logs is to give you an idea of the nature of traffic against your bucket."
We plan to strengthen the requirements below in future versions.

TODO: confirm with Core42

Best-effort delivery:
Asynchronous delivery (up to 2 hours latency in nominal conditions)
No guarantee that logs will not be lost or duplicated
Changes to logging configuration can take time to propagate
Logs may arrive late. If the system fails to collect a log record in time to include it in the log object corresponding to its "time window", it can appear in a later log object (as long as the log events in the object are ordered by event time). 
Log records inside a log object are ordered by time; there is no ordering guarantee across different log objects.

Additional requirements:
The feature can be disabled and is disabled by default.
When enabled, it should have limited performance impact.
When disabled, it must have no performance impact
The system must scale to support logging enabled for all buckets.

Requirements in Citadel

Proposal
General Architecture

CloudServer (and future producers) writes a structured log record per API operation to a new, dedicated file (separate from the CloudServer process logs). 

Fluent Bit tails the file on each node and ships log records to ClickHouse through the existing forward proxy. 

ClickHouse stores log records for a limited time, as a buffer.

A consumer service (called Bucket Logging Processing, BLP for short), written in Go, reads log records from ClickHouse, builds log objects, and writes them to destination buckets via the external S3 API.


Common ingest path with S3 Analytics
TODO: Add example of logs from cloudserver
TODO: custom logs


We use a common log record ingest path with S3 analytics.
Fluent Bit reads only from the new log file, dedicated to Bucket Server Access Logging and S3 Analytics, and not from the CloudServer process logs, and inserts into ClickHouse.

This decision was made because:
Avoids doubling ClickHouse inserts.
Simplifies Fluent Bit configuration (no need to filter and parse CloudServer process logs).
Improves robustness by using purpose‑built log records instead of process logs. Allows us to add tests to ensure that each API creates the expected log record.


ClickHouse schema

Fluent Bit inserts log records into an ingest table that does not store data. 

Each insert into the ingest table triggers two materialized views:
The existing S3 Analytics materialized view.
A view that inserts log records with bucket logging enabled into the table that stores log records (`logs` table).

Ingest table (does not store data; local table on every ClickHouse node)

CREATE TABLE IF NOT EXISTS logs.access_logs_ingest ON CLUSTER main_cluster
(
    timestamp              DateTime,
    startTime              DateTime,
    hostname               LowCardinality(String),
    action                 LowCardinality(String),
    accountName            String,
    accountDisplayName     String,
    bucketName             String,
    bucketOwner            String,
    userName               String,
    requester              String,

    httpMethod             LowCardinality(String),
    httpCode               UInt16,
    httpURL                String,
    errorCode              String,
    objectKey              String,

    versionId              String,

    bytesDeleted           UInt64,
    bytesReceived          UInt64,
    bytesSent              UInt64,
    bodyLength             UInt64,
    contentLength          UInt64,

    clientIP               String,
    referer                String,
    userAgent              String,
    hostHeader             String,

    elapsed_ms             Float32,
    turnAroundTime         Float32,

    req_id                 String,
    raftSessionId          UInt16,

    signatureVersion       LowCardinality(String),
    cipherSuite            LowCardinality(String),
    authenticationType     LowCardinality(String),
    tlsVersion             LowCardinality(String),
    aclRequired            LowCardinality(String),

    logFormatVersion       LowCardinality(String),
    loggingEnabled         Bool,
    loggingTargetBucket    String,
    loggingTargetPrefix    String,

    insertedAt             DateTime DEFAULT now()
)
ENGINE = Null();

inserted_at: Operation timestamps are unreliable for ordering because logs can be inserted with delay. Repeating a query with the same time window may yield different results (no idempotence). To address this, we use a monotonically increasing timestamp representing the time of insertion into ClickHouse. ClickHouse auto-populates it.

Materialized view that filters log records with loggingEnabled = true

CREATE MATERIALIZED VIEW IF NOT EXISTS logs.access_logs_ingest_mv ON CLUSTER main_cluster
TO logs.access_logs_federated
AS
SELECT *
FROM logs.access_logs_ingest
WHERE loggingEnabled = true;


Logs table

Local tables (on every ClickHouse node)

CREATE TABLE IF NOT EXISTS logs.access_logs ON CLUSTER main_cluster
AS logs.access_logs_ingest
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}'
)
PARTITION BY toStartOfDay(insertedAt)
ORDER BY (raftSessionId, bucketName, insertedAt, req_id)
TTL insertedAt + INTERVAL 3 DAY DELETE

ORDER BY (raftSessionId, bucketName, insertedAt, req_id): The consumer query filters by raftSessionId and uses GROUP BY bucketName (see below). This sorting ensures the query is efficient.
PARTITION BY toStartOfDay(insertedAt): ClickHouse creates a separate directory per day. We choose this because it gives the following properties:
- Fast partition deletion (works well with TTL policy)
- Fewer large parts are more efficient for ClickHouse background merges.

TTL insertedAt + INTERVAL 3 DAY DELETE: Automatically deletes data older than 3 days.

Distributed table

CREATE TABLE IF NOT EXISTS logs.access_logs_federated ON CLUSTER main_cluster
AS logs.access_logs
ENGINE = Distributed(main_cluster, logs, access_logs, raftSessionId);

Distributed(main_cluster, logs, access_logs, raftSessionId):
Sharding key options considered: raftSessionId vs bucketName.
Given that the number of raft sessions vastly outnumber ClickHouse shards (at Core42: 29 MD clusters * 12 raft sessions per cluster), both options are equivalent and provide the same query locality (each bucket maps to a single ClickHouse shard).
We choose raftSessionId to benefit from the introduction of bucket sharding in Metadata in the future (high-traffic buckets can be spread across raft sessions -> can also be spread across ClickHouse shards).

Tracking progress

We track processed logs using offsets stored in a separate ClickHouse table.

CREATE TABLE IF NOT EXISTS logs.offsets ON CLUSTER main_cluster
(
    bucketName            String,
    raftSessionId         UInt16,
    last_processed_ts     DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}',
    last_processed_ts
)
PRIMARY KEY (bucketName);

CREATE TABLE IF NOT EXISTS logs.offsets_federated ON CLUSTER main_cluster
AS logs.offsets
ENGINE = Distributed(main_cluster, logs, offsets, raftSessionId);

Storage requirements
For the calculations below, we use data from one of the rings at Core42:
The system has 177 nodes. CloudServer runs on every node.
Target performance is 360 GB/s read throughput and 151 GB/s write throughput, for 10 MB objects.

To achieve the combined throughput, the system needs to support ~52k ops/s:

(360 GB/s + 151 GB/s) * 1024 MB/GB = 523,264 MB/s
523,264 MB/s / 10 MB/op = 52,326 ops/s

We assume:
Average log record size: 900 bytes.
Replication factor: 3
Compression ratio: 3x

Based on those assumptions, ~2.47 TB of data will be inserted into the ClickHouse cluster per day:
52,326 logs/s * 900 bytes/log = 47,093,400 bytes/s ~= 44.9 MB/s
44.9 MB/s ~= 3,879,000 MB/day ~= 3.70 TB/day
3.7 TB/day * 3 (replication factor) = 11.1 TB/day
11.1 TB/day / 3 (compression ratio) ≈ 3.7 TB/day


With a 6-node ClickHouse cluster, each node needs ~632 GB of additional storage space per day for access logs.

TODO: actual disk space with realistic load (flush?)
TODO: CPU during tests

TODO: read-only mechanism
Retention

We must retain log records long enough for the consumer to process them. This must cover consumer outages that require a fix to be deployed.

We use a TTL (time to live) policy that drops data parts from ClickHouse after a specified retention window.

To support a 3-day TTL, each ClickHouse node requires ~2 TB of disk space.

Consumer service (Bucket Log Processor - BLP)

TODO: bucket sharding - future considerations

The consumer fetches logs from ClickHouse, builds log objects, and writes them to destination buckets.
Work sharding

We deploy multiple consumer instances. Each instance processes a distinct, non-overlapping subset of raft sessions.

By default, the number of deployed instances equals the number of raft sessions, so each consumer instance is responsible for one raft session.

Instances are deployed with Ballot (see “Options considered” section below).

Scaling the number of instances up or down requires stopping all instances, updating configuration, and then restarting them.
Consumer algorithm

The consumer operates in two phases:

Work discovery phase

At a configured interval, the consumer runs a ClickHouse query to identify batches of log records ready for processing.

The work discovery query:
Selects buckets owned by this BLP instance.
Uses the offsets table to compute, per bucket, the count of unprocessed logs and the oldest/newest unprocessed timestamps.
Selects buckets ready based on either:
Log count threshold: unprocessed logs >= configurable threshold
Time threshold: the oldest unprocessed log is older than a configurable threshold

The output is a list of work orders: (Bucket, min_ts, max_ts).

Work discovery query

WITH
    -- Find the most recent timestamp processed for each bucket
    bucket_offsets AS (
        SELECT
            bucketName,
            -- max() ensures we get the most recent processed
               timestamp for each bucket
            -- Note: FINAL could be used here, but did not work
               correctly when tested
            max(last_processed_ts) as last_processed_ts
        FROM logs.offsets
        GROUP BY bucketName
    ),

    -- Pre-filter logs for this specific server instance
    instance_logs AS (
        SELECT
            bucketName,
            insertedAt
        FROM logs.access_logs
        WHERE
            -- Sharding clause
             raftSessionId % ${totalInstances} = ${instanceID}
    ),

    -- Find unprocessed logs for each bucket
    new_logs_by_bucket AS (
        SELECT
            l.bucketName,
            count() AS new_log_count,         -- How many unprocessed
                                                 logs exist for this
                                                 bucket
            min(l.insertedAt) as min_ts,      -- Oldest unprocessed
                                                 log timestamp
            max(l.insertedAt) as max_ts       -- Newest unprocessed
                                                 log timestamp
        FROM instance_logs AS l
        LEFT JOIN bucket_offsets AS o ON l.bucketName = o.bucketName
        -- Include logs that are either:
        -- 1. Newer than the last processed timestamp, OR
        -- 2. From buckets we've never processed before (o.bucketName IS
           NULL)
        WHERE l.insertedAt > COALESCE(o.last_processed_ts, '1970-01-01 00:00:00')
        GROUP BY l.bucketName
    )
-- Main query: Select buckets ready for batch processing
-- A bucket is "ready" if it meets EITHER condition:
-- 1. Log count threshold: has enough logs to justify processing cost
-- 2. Time threshold: has logs old enough that they should be processed regardless of count
SELECT
    bucketName,
    new_log_count,
    min_ts,
    max_ts
FROM new_logs_by_bucket
WHERE new_log_count >= ${countThreshold}                                -- Volume condition
    OR min_ts <= now() - INTERVAL ${timeThresholdSeconds} SECOND        -- Age condition


Log fetch and processing phase

For each work order, the consumer fetches logs in the time window.
It parses the fetched logs, builds a log object, writes it to the destination bucket, and commits the offset by inserting into the offsets table.

Backpressure

Backpressure can occur in two places:
Inserts to ClickHouse fail (ClickHouse outage) or are slow. Logs then accumulate in Fluent Bit buffers (see filesystem buffering below).
The consumer is down or slower than log generation. Unprocessed logs may be deleted by the ClickHouse TTL policy.

Strategy:
Use fixed retention limits in Fluent Bit and ClickHouse; do not retain logs indefinitely.
Tune TTL and Fluent Bit storage limits to balance tolerance to outages with storage use.
Monitor lag in both areas and alert when thresholds are reached.
Additional configuration for Fluent Bit
Filesystem buffering
Currently, we use in-memory buffering.
Reasons for using filesystem buffering:
Prevents log data loss in case of crash or restart.
It can handle backpressure for longer compared to memory.

To limit the total size of queued filesystem chunks, Fluent Bit v1.6 and later provides storage.total_limit_size, which caps the total size per output destination.

This configuration parameter affects:
The storage space that Fluent Bit can use on the system.
The amount of time that the system can handle a ClickHouse outage before losing log records.

On each server, Fluent Bit ingests 260 KB/sec
52,326 logs/s / 177 servers = 296 logs/s
296 logs/s * 900 bytes/log = 266,400 bytes/s

This results in ~0.9 GB per hour:
260 KB/sec * 3600 = 936,000 KB/hour ~= 914 MB/hour

Setting storage.total_limit_size to 10 GB allows Fluent Bit to tolerate a 10-hour ClickHouse downtime before log data loss occurs. 
Tail input database file
With this configuration, Fluent Bit persists the offset in the tailed file in an sqlite database.
Reasons for using a database file:
Prevents log data loss or duplication on restarts: Without a database file, Fluent Bit starts reading the file from either the beginning or the end after a restart.
Reliably track rotated files.

Other aspects
Circuit-breaker

We already use a circuit breaker mechanism to prevent ClickHouse from growing beyond a configured size limit (default 100GiB).

A script periodically checks the size of the logs database.
If the size exceeds the configured limit, the script revokes write access from Fluent Bit.
If the size is lower than the configured limit, and Fluent Bit does not have write access, the script grants Fluent Bit access.

The circuit breaker covers all tables in the logs database so it will cover the new access logging tables.
The script runs on each server and checks the database size on that specific server.  

The default 100GiB needs to be increased, as we will be storing access logs, not just aggregations, for access logging.
We will:
Keep the 100 GiB limit when access logging is disabled
Set a sensible default when access logging is enabled. It needs to be balanced to avoid hitting the limit too soon, and also not use too much space on servers that run ClickHouse.
We will document a formula for calculating the limit.
Custom access log information

AWS allows users to include custom information in the access log record for a request.
To do this, users need to add custom query-string parameters to the request URL.
Query-string parameters starting with “x-” are ignored by S3, but included in the access log record as part of the “Request-URI” field.

Options considered and decisions
Kafka-based architecture
An alternative design that uses Kafka was considered.
In this alternative design, CloudServer would write log records to a Kafka topic, and consumers would read from Kafka.

This approach was rejected for the following reasons:
Offset commit complexity
Kafka partitions are strictly ordered streams that interleave multiple buckets. This conflicts with per-bucket processing.
Consider the following scenario:
A consumer reads a batch of 100 messages from a Kafka partition.
These messages contain logs for buckets A, B, and C.
The consumer processes logs for bucket A and successfully writes the log object.

The consumer cannot commit a new offset to Kafka. It can only commit after it has processed all three buckets.
This forces either reprocessing on failure or complex external offset management.

Inefficient batch formation
With skewed traffic, batches read from Kafka would contain mostly logs from high‑traffic buckets and few logs from other buckets. The consumer can either create inefficient small objects for low‑traffic buckets or implement in‑memory buffering, which increases complexity.

Kafka-based shipping
In this alternative design, Kafka is used to ship logs to ClickHouse (instead of local file + Fluent Bit).

This approach provides better robustness, but includes some considerations (backpressure in case of Kafka outage, log record retention in Kafka).

We did not choose this for the first iteration due to the delivery timeline (we consider that re-using the local file + FluentBit approach of S3 Analytics will allow faster implementation).
We consider using this approach in a later version. 
Consumer service deployment (open question)

Option 1: Use Ballot

Ensures that a single consumer instance is running per raft session. Handles failover.

Issue/risk

Multiple components establish connections to Zookeeper, and the number of connections is a function of the number of nodes in the system and the number of Raft sessions.

This has already created an issue during installation at Core42.

Using Ballot to deploy the instances of the consumer service will exacerbate this issue.

To address this, we can utilize the observation that there is no need for 177 nodes to be candidates for every consumer instance. Instead of making all 177 nodes participate in leader election for a given consumer instance, restrict the election to a small pool (e.g. 5 nodes).
Example:
Raft session1 -> candidates: node1, node2, node3, node4, node5
Raft session2 -> candidates: node6, node7, node8, node9, node10
…

TODO: define strategy for distributed ballot candidates to nodes

Option 2: Use a mechanism similar to BackBeat’s ProvisionDispatcher to assign raft sessions to consumer instances (also uses Zookeeper).

Filtering log records with bucket logging enabled
Assumptions:
Bucket logging configuration is stored in the metadata of each bucket.
CloudServer already reads the metadata of the bucket for all operations that refer to a bucket. 

Option 1. CloudServer does not generate log records when logging is disabled for a bucket.

Rejected because it does not allow a unified log record flow with S3 Analytics.

Option 2. No bucket logging configuration is contained in log records; filter in the consumer service.

Rejected because:
Results in storing log records that should not be delivered to ClickHouse.
Adds complexity to consumer service because it needs to discover bucket logging configuration.


Option 3 (chosen). Include logging configuration in each log record.

Reasons for choosing this approach:
No additional cost in CloudServer.
Simplifies the consumer.
No configuration propagation delay.

Trade-off: Resources consumed by the materialized view that filters out log records with bucket logging disabled.
Alternative work partitioning mechanism for the consumer

Instead of having every consumer instance poll ClickHouse to discover work, we can have a “scheduler” process that queries ClickHouse and creates tasks, and a pool of “worker” processes that pick up and execute tasks.

Advantage: Would partition work more efficiently. Each log batch can be independently processed.

Adds complexity for implementing or introducing a task queue. Can be considered for a later version.

Known limitations
Logs can be lost in the following cases:
CloudServer crash: Log records buffered in memory, not yet written to the log file, will be lost.
ClickHouse replication leader crash: We currently use asynchronous replication in ClickHouse.
  
The consumer will deliver duplicate log records in case of a crash. We can consider this acceptable for the first version. We plan to add an idempotence mechanism in a later version.

Performance evaluation
Ingestion performance
The goal is to evaluate the data ingestion capabilities of the ClickHouse cluster.

Objectives
Determine the maximum sustainable ingestion throughput by measuring the system's scalability under increasing concurrent load (workers) and identifying the saturation point.
Quantify the performance overhead of replication and the scalability benefits of sharding.

Test environment
7-node lab on AWS (Supervisor: t3.xlarge, Storage nodes: t3.2xlarge)

RING version
9.5.0.0

Methodology
Each test creates new ClickHouse tables.
Local table: insert into the ingest table -> materialized view -> local table on 1 node
Replicated table / 1 shard: insert into the ingest table -> materialized view -> 1 shard replicated across 3 nodes
Distributed table: 2 shards (6 nodes)
Tests run workers on storage nodes (we run multiple test runs with an increasing number of workers).
Each worker performs inserts into Clickhouse with a specified batch size (number of log records)

Test scenarios
1. Local table (single-node baseline)
Objective: Establish the maximum ingestion performance of a single, non-replicated ClickHouse node
ClickHouse topology: A single logs table on one ClickHouse node.
Data Flow: Workers insert into the ingest table, which triggers a materialized view to write the filtered data to the local logs table on that same node.
All insert traffic is directed to a single node. Workers are distributed across the other 5 nodes in the cluster.


2. Replicated table (single-shard baseline)
Objective: Measure the performance limit of a single shard (3 replicas) and quantify the overhead of replication.
ClickHouse topology: 1-shard, 3-replica cluster.
Data flow: Same as the local, but the final write is to a replicated table.
Worker placement: Workers are distributed across the three nodes that are not part of the ClickHouse shard.


3. Distributed table (2 shards)
Objective: Measure the total ingestion throughput of the 2-shard cluster and identify any coordination or distribution bottlenecks.
ClickHouse topology: A Distributed table configured with 2 shards, each with 3 replicas
Data flow:
Workers insert into the local ingest table on one of the 6 nodes.
The materialized view writes filtered results to the distributed logs table.
The distributed table forwards to the correct shard based on the sharding key (hash of bucket name)
Worker placement: Workers are distributed across all 6 nodes of the cluster.
  

Observations
Local table:
Sustainable peak throughput: ~187k rows/sec.
Can sustainably support 18 workers.
Replicated table
Sustainable peak throughput: ~200k rows/sec
Can sustainably support 24-30 workers.
Distributed table
Sustainable peak throughput: ~234k rows/sec
Can sustainably support 30-36 workers.


Remaining Work: Consumer Query Performance Evaluation
Objective: Validate the efficiency and scalability of the consumer's "find work" query.
Data volume scalability test: Pre-populate the logs table with increasingly large datasets. Verify that the query's execution time remains consistently low.
Bucket Cardinality scalability Test: With a fixed, large dataset, vary the number of unique buckets.

Sizing ClickHouse

Cluster size can be determined by three independent requirements. 
We must calculate the resources needed for each, and provision for the largest of the three.
Storage capacity: How much disk space is required per server to store the data for the specified TTL.
Ingestion throughput: How many shards are needed to handle the peak write load.
Query performance: How many shards are needed to provide fast consumer queries.

Storage capacity:
Based on the calculation in an earlier section
6-node ClickHouse cluster: Each node needs ~632 GB per day for access logs
9-node ClickHouse cluster: Each node needs ~422 GB per day for access logs.
12-node ClickHouse cluster: Each node needs ~315 GB per day for access logs.
Note: consider dedicated server for ClickHouse
Risks
Incomplete preliminary performance evaluation

Due to the time constraints for delivering this feature at Core42.

Mitigation: Use the help of InstincTools and plan development to enable tests to start as early as possible.
Delivery in phases

First version delivers the core functionality; follow-up versions improve robustness and add features:
ACLs
Custom logs
Log object key format options
Consumer idempotence (no duplicate logs in case of consumer crash)
Scale consumers up/down without downtime



