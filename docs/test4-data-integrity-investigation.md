# Data Integrity Test - Investigation Report

**Lab:** itools.scality.internal
**RING version:** 9.5.2 GA
**Confluence page:** https://scality.atlassian.net/wiki/spaces/RING/pages/3861315652

## Test Setup (common to all runs)

- 20 buckets with bucket logging enabled
- 150,000 objects created per bucket (3,000,000 total)
- Config: `count_threshold: 5000`, `time_threshold_seconds: 300`, `max_logs_per_bucket: 100000` (default), `retention_hours: 1`
- 6 storage nodes: md1 (stor1) through md5 (stor5) + wsb1 (stor6)

---

## Test Run 1 (2026-03-13)

**Bucket prefix:** `20260313165743`
**Investigation date:** 2026-03-16

### Result

- **Expected:** 3,000,000 PutObject log entries delivered
- **Delivered:** 2,999,292
- **Missing:** 708
- **Duplicates:** 0
- **Verdict:** FAILED (threshold: max 1 lost per 1.5M)

## Proven Facts

### 1. CloudServer wrote all 3,000,000 log entries

Counted PutObject entries matching `"tester"` account and `"20260313165743"` prefix in `server-access.log*` files on every node:

| Node | Buckets | PutObject count |
|------|---------|-----------------|
| md1 (stor1) | 0, 6, 12, 18 | 600,000 |
| md2 (stor2) | 1, 7, 13, 19 | 600,000 |
| md3 (stor3) | 2, 8, 14 | 450,000 |
| md4 (stor4) | 3, 9, 15 | 450,000 |
| md5 (stor5) | 4, 10, 16 | 450,000 |
| wsb1 (stor6) | 5, 11, 17 | 450,000 |
| **Total** | | **3,000,000** |

**Conclusion:** Zero loss at CloudServer level. Every PutObject operation was logged.

### 2. Log-courier delivered 2,999,292 PutObject entries

Downloaded and parsed all 1,592 log objects from `20260313165743-target-bucket`. Per-bucket breakdown:

| Bucket | Delivered |
|--------|-----------|
| bucket-0 through bucket-8 | 150,000 each |
| **bucket-9** | **149,292** |
| bucket-10 through bucket-19 | 150,000 each |
| **Total** | **2,999,292** |

**Conclusion:** All 708 missing entries are from a single bucket (`bucket-9`).

### 3. bucket-9 is served exclusively by md4-cluster1 (stor4)

Per-node per-bucket counts from server-access.log confirm each bucket's PutObject entries exist on exactly one node. bucket-9's 150,000 entries are all on md4-cluster1.

**Conclusion:** The loss is isolated to a single node.

### 4. The 708 missing entries form a single contiguous range

Compared expected object keys (`obj-0` through `obj-149999`) against delivered keys for bucket-9:

- **Missing range:** `obj-135677` through `obj-136384` (708 consecutive objects)
- **Number of contiguous gaps:** 1
- **Entries delivered after the gap:** 13,615 (`obj-136385` through `obj-149999`)

**Conclusion:** This is not a tail drop. Data was lost in the middle, with successful delivery resuming immediately after.

### 5. Log-courier reported zero errors

Searched log-courier logs on the leader node (md5/stor5) for March 13. Every `batch processing completed` message shows `failed:0`. No error, warning, or retry messages found.

### 6. Fluent Bit reported zero errors

Searched all Fluent Bit logs (internal application logs, service logs, proxy logs) on all 6 nodes for March 13. No errors or warnings found. All HTTP responses to ClickHouse proxy were 200.

### 7. Fluent Bit had no restarts on stor4

Full Fluent Bit logs on md4-cluster1 for March 13 contain only inotify watch add/remove events from log rotations. No startup messages, no reconnection events, no restarts.

### 8. ClickHouse data is unavailable

The `access_logs` table has a 24-hour TTL (`retention_hours: 1` in config). By the time of investigation (March 16), all March 13 data has been purged. We cannot verify how many entries ClickHouse actually received from Fluent Bit.

### 9. Logrotate configuration for server-access.log

Found at `/scality/ssd01/s3/scality-s3-analytics/logrotate/logrotate-fluentbit.conf`:

```
/logs/s3/server-access.log
{
    create 644 scality scality
    missingok
    notifempty
    rotate 7
    compress
    delaycompress
    daily
    maxsize 10M
}
```

- Uses `create` mode (rename old file, create new one) — NOT `copytruncate`
- `maxsize 10M` triggers rotation when the file exceeds 10MB
- `delaycompress` delays compression by one rotation cycle
- Logrotate runs every 600 seconds (10 minutes) via a wrapper script

### 10. Fluent Bit configuration

Key settings from `/scality/ssd01/s3/scality-s3-analytics/conf/fluent-bit.conf`:

| Setting | Value | Significance |
|---------|-------|-------------|
| `rotate_wait` | 30 seconds | Time FB continues reading old file after rotation |
| `Flush` | 10 seconds | Interval for sending data to output |
| `Retry_Limit` | 5 | Output retries before discarding a chunk |
| `Mem_Buf_Limit` | not set | Unlimited memory buffer — no backpressure pausing |
| `Read_from_Head` | true | On first start, reads from beginning of file |
| `DB` | `/fluent-bit/data/tail.db` | Tracks file offsets across restarts |

The config contains this comment:

> *"If rotate_wait is lower than checkFileRotationIntervalMS fluentbit may stop reading before cloudserver switches files and we may lose logs."*

This shows the developers are aware of rotation-related loss as a risk.

### 11. Log rotations occurred during the test window on stor4

Fluent Bit rotation events logged on md4-cluster1 during the test period:

```
[2026/03/13 16:28:46] handle rotation(): inode=272128954
[2026/03/13 16:38:46] handle rotation(): inode=272129133
[2026/03/13 16:48:46] handle rotation(): inode=272128954
[2026/03/13 17:08:53] handle rotation(): inode=272129133
[2026/03/13 17:18:56] handle rotation(): inode=272128954
```

Two inodes alternate, indicating two S3 server processes writing access logs. Rotations occurred approximately every 10 minutes during the test.

### 12. No data loss on any other node

All 5 other nodes delivered exactly 150,000 entries per bucket. Rotations also occurred on those nodes during the same period (verified in Fluent Bit logs). Whatever caused the loss on stor4 did not affect the other nodes.

### 13. The missing entries are in the MIDDLE of a log file, not at the tail

The 708 missing bucket-9 entries exist in `server-access.log.4.gz` on stor4:

| Metric | Value |
|--------|-------|
| Total lines in `.log.4.gz` | 134,656 |
| First missing entry (`obj-135677`) at line | 92,891 |
| Last missing entry (`obj-136384`) at line | 95,031 |
| Lines after last missing entry | ~39,625 |

The file contains bucket-9 entries from `obj-104778` to `obj-149999`, with the missing range in the middle. Entries before AND after the gap in the same file were successfully delivered.

**Conclusion:** This rules out `rotate_wait` timeout as the loss mechanism. If Fluent Bit had stopped reading the old file too early, the missing entries would be at the end of the file, not in the middle.

### 14. Timestamps of the missing entries

The missing entries span unix timestamps 1773421881 to 1773421884 (a ~3 second window).

---

## Test Run 2 (2026-03-17)

**Bucket prefix:** `20260317113335`
**Investigation date:** 2026-03-17

### Result

- **Expected:** 3,000,000 PutObject log entries delivered
- **Missing:** 3,974
- **Duplicates:** 0
- **Verdict:** FAILED

### Missing entries by bucket

| Bucket | Missing | Gaps |
|--------|---------|------|
| bucket-9 | 1,428 | obj-107092–107804 (713), obj-108514–109228 (715) |
| bucket-3 | 1,410 | obj-105868–106570 (703), obj-107288–107994 (707) |
| bucket-10 | 1,136 | obj-147862–148997 (1,136) |

Same pattern as run 1: contiguous gaps in the middle, multiple affected buckets this time.

### Proven Facts (Run 2)

#### 15. Missing entries existed in ClickHouse

Verified during live investigation that all entries for affected buckets existed in ClickHouse (checked before TTL expiration). Each bucket had the expected number of entries in `access_logs_federated`. The data loss occurs after ClickHouse ingestion — the issue is in log-courier.

Subsequent queries for the specific missing objectKeys returned empty results because the 1-hour TTL (`retention_hours: 1`) had expired the data by the time we ran them.

#### 16. The LIMIT does not cut within a single bucket

Each bucket has ~36,000–40,000 entries in ClickHouse. With `max_logs_per_bucket: 100000` (default), all entries for a single bucket fit within a single LIMIT. The LIMIT never activates for any bucket in this test configuration.

This rules out the "LIMIT cuts through a same-insertedAt group" hypothesis as the sole explanation.

#### 17. `startTime` is not monotonically correlated with `insertedAt`

Queried the `startTime` range within each `insertedAt` second for bucket-18:

| insertedAt | entry_count | startTime range (seconds) |
|------------|-------------|---------------------------|
| 11:48:38 | 2,576 | 20s (11:48:18 → 11:48:38) |
| 11:48:28 | 2,093 | 15s |
| 11:48:18 | 2,074 | 15s |
| 11:47:27 | 1,535 | 10s |

Within a single `insertedAt` second, `startTime` values span 10–20 seconds. S3 requests from different clients are batched by Fluent Bit and assigned the same `insertedAt` by ClickHouse's `now()` at insert time.

This breaks an assumption in the offset mechanism: the composite offset `(insertedAt, startTime, reqID)` with strict `>` comparison assumes entries within an `insertedAt` second are ordered by `startTime`. In practice, a single `insertedAt` second contains entries with widely varying `startTime` values.

#### 18. ClickHouse table ORDER BY differs from LogFetcher ORDER BY

The `access_logs` table definition:
```
ORDER BY (raftSessionID, bucketName, insertedAt, startTime, req_id)
```

LogFetcher and BatchFinder query:
```
ORDER BY insertedAt ASC, startTime ASC, req_id ASC
```

The table's primary key includes `raftSessionID` and `bucketName` before the time columns. The query ORDER BY does not match the table's sort order, which means ClickHouse cannot use the primary index for ordering and must re-sort the results.

## Ruled Out Hypotheses

### `rotate_wait` timeout

If Fluent Bit's `rotate_wait` (30s) expired before it finished reading the old file, the missing entries would be at the **tail** of a rotated file. Fact #13 proves the missing entries are in the **middle** of `.log.4.gz`, with ~39,625 lines successfully read after them. This rules out `rotate_wait` as the mechanism.

### `copytruncate` data loss

The logrotate config uses `create` mode, not `copytruncate` (fact #9). This rules out the classic copytruncate race condition.

### Fluent Bit restart

No restart events found in Fluent Bit logs on stor4 for March 13 (fact #7).

### Log-courier LIMIT + offset skip

The original hypothesis was that `max_logs_per_bucket: 100000` (LIMIT) cuts through a group of entries sharing the same `insertedAt` second, and the committed offset causes entries in the second half to be skipped.

Fact #16 disproves this for the current test configuration: each bucket has ~40k entries, well under the 100k LIMIT. The LIMIT never activates. A test was written (`logfetch_test.go`: "should not lose logs when LIMIT cuts a same-insertedAt group across cycles") that exercises the full cycle (BatchFinder → LogFetcher → offset commit → BatchFinder → LogFetcher). With static data and sequential `startTime` values, the test passes — LIMIT pagination works correctly when entries sort deterministically.

However, the root cause likely still involves the offset mechanism (fact #15 confirms the loss is in log-courier). The specific mechanism remains under investigation.

## Remaining Hypotheses

### 1. Out-of-order data visibility in ClickHouse (PRIMARY)

**Status:** Mechanism identified from code analysis. Awaiting data from Run 3 to confirm.

#### The mechanism

The triple composite offset `(insertedAt, startTime, reqID)` is a monotonic cursor — it assumes that once offset O is committed, no record can ever appear with a tuple ≤ O. This requires **monotonic visibility**: all records with `insertedAt ≤ T` must be queryable before any record with `insertedAt > T` becomes queryable.

ClickHouse does not provide this guarantee. The data pipeline is:

```
Fluent Bit → access_logs_ingest (Null) → MV → access_logs_federated (Distributed) → access_logs (ReplicatedMergeTree)
```

`insertedAt` is `DateTime` (second precision) with `DEFAULT now()`, evaluated on the **source node** at insert time and preserved through the pipeline. By the time data reaches the target shard, physical visibility order can differ from `insertedAt` order due to:

1. **Distributed table async delivery**: The MV inserts into the Distributed table, which routes by `raftSessionID`. When the target shard is remote, data goes through an async buffer. Deliveries from different inserts are independent and not ordered.
2. **ReplicatedMergeTree replication lag**: Each INSERT creates a part. Parts replicate between replicas independently. A newer part can replicate before an older part.

When later-inserted data (insertedAt=T2) becomes visible before earlier-inserted data (insertedAt=T1, where T1 < T2), log-courier processes the T2 data and commits offset (T2, ...). When the T1 data arrives, the filter rejects it:

- `insertedAt > T2` → T1 > T2 → **FALSE**
- `insertedAt = T2 AND startTime > ...` → T1 = T2 → **FALSE**
- `insertedAt = T2 AND startTime = ... AND req_id > ...` → T1 = T2 → **FALSE**

All three conditions fail because `insertedAt = T1 < T2`. The data falls into a blind spot — neither greater than nor equal to the committed offset's `insertedAt`. Records are permanently and silently skipped.

#### Concrete example

**Setup:** Bucket-9 on stor4, raftSessionID routes to a remote shard. Fluent Bit flushes every 10s. Current committed offset: `(insertedAt=100, startTime=100.500, reqID="req-A")`.

**Step 1 — Two consecutive Fluent Bit flushes:**

| Flush | insertedAt | Records for bucket-9 (startTime, reqID, objectKey) |
|-------|-----------|-----------------------------------------------------|
| T=110 | 110 | (95.200, req-B, obj-135677), (102.300, req-C, obj-135900), (108.700, req-D, obj-136384) |
| T=120 | 120 | (105.100, req-E, obj-136385), (115.400, req-F, obj-136700), (119.900, req-G, obj-137000) |

Both go through the Distributed engine's async buffer to the remote shard.

**Step 2 — Out-of-order delivery:** The T=120 data arrives at the target shard first. The T=110 data is still in transit.

**Step 3 — Log-courier queries:** Sees only the T=120 records (T=110 not yet visible). Fetches them, uploads to S3, commits offset `(120, 119.900, "req-G")`.

**Step 4 — T=110 data arrives.** Now visible in ClickHouse.

**Step 5 — Next log-courier query** with offset `(120, 119.900, "req-G")`:

| Record | ① insertedAt > 120? | ② insertedAt = 120? | Result |
|--------|---------------------|---------------------|--------|
| (110, 95.200, req-B) | 110 > 120 → NO | 110 = 120 → NO | **EXCLUDED** |
| (110, 102.300, req-C) | 110 > 120 → NO | 110 = 120 → NO | **EXCLUDED** |
| (110, 108.700, req-D) | 110 > 120 → NO | 110 = 120 → NO | **EXCLUDED** |

**Result:** obj-135677 through obj-136384 are permanently skipped. Zero errors raised. Zero duplicates.

#### Why it fits all observations

| Observation | Explained? |
|---|---|
| Contiguous gap in the middle (fact #4) | One Distributed delivery's worth of records arrived late |
| Entries before AND after gap delivered (fact #4) | Earlier flushes already delivered; later flush arrived first and was delivered |
| Zero errors in all components (facts #5, #6) | Offset filter silently excludes records |
| Zero duplicates (both runs) | No data is re-processed |
| Loss confirmed in log-courier (fact #15) | Data existed in ClickHouse but was skipped by the offset filter |
| startTime spans 10–20s within one insertedAt (fact #17) | Wide startTime spread means skipped records have tuples far below the committed offset |
| Multiple nodes affected in Run 2 | Systemic to any data path involving async delivery or replication lag |
| Reproducible across runs | Out-of-order visibility is a systemic property of distributed ClickHouse |

#### Testable prediction

**The missing records will have `insertedAt` values strictly less than the `insertedAt` of records delivered around the same time.** Specifically, there will be one or more `insertedAt` seconds where 100% of records are missing, surrounded by `insertedAt` seconds where 100% of records are delivered. This is the signature of a skipped Distributed delivery.

### 2. Fluent Bit output retry exhaustion

Fluent Bit has `Retry_Limit 5`. If the ClickHouse proxy returned a non-200 response for a specific batch, FB would retry 5 times then silently discard the chunk. This would cause a contiguous gap in the middle of a file (the discarded chunk) while data before and after is delivered successfully.

Evidence for: Fits the pattern (contiguous gap, mid-file, single node).
Evidence against: All HTTP responses logged as 200 (fact #6). However, FB only logs successful responses — failed responses may not be logged at `info` level. Also, fact #15 confirms data existed in ClickHouse, which rules out FB-level loss.

### 3. Fluent Bit internal chunk handling bug

Under high throughput, FB could have a bug where a chunk is lost during internal processing (parsing, filtering, buffering). This would produce a mid-file gap.

Evidence for: Fits the pattern.
Evidence against: No errors logged, but FB may not detect or report this condition. Also ruled out by fact #15 — data was in ClickHouse.

### 4. ClickHouse async insert failure

The Fluent Bit output inserts with `async_insert=1, wait_for_async_insert=1`. ClickHouse could have accepted the HTTP request (200) but failed to materialize the async insert. This would mean the data was acknowledged but never actually stored.

Evidence for: Would explain a mid-file gap with no errors in any component. The 200 response from the proxy means FB considers the data delivered.
Evidence against: `wait_for_async_insert=1` should make ClickHouse wait until the insert is flushed before responding, reducing this risk. Also ruled out by fact #15.

## Next Steps

### 1. Reproduce with longer ClickHouse TTL and verify the testable prediction

Re-run the data integrity test with `retention_hours: 24` to preserve ClickHouse data for post-test analysis.

#### 1a. Identify missing records

Same method as Runs 1 and 2: diff expected object keys against delivered object keys to produce the list of missing keys per bucket.

#### 1b. Query the `insertedAt` distribution for affected buckets

For each affected bucket, get the per-`insertedAt`-second breakdown:

```sql
SELECT
    insertedAt,
    count() AS record_count,
    min(startTime) AS min_startTime,
    max(startTime) AS max_startTime,
    min(objectKey) AS min_objectKey,
    max(objectKey) AS max_objectKey
FROM logs.access_logs_federated
WHERE bucketName = '<affected-bucket>'
  AND raftSessionID = <id>
GROUP BY insertedAt
ORDER BY insertedAt
```

This shows which `insertedAt` seconds contain which objectKey ranges and how many records each has.

#### 1c. Query the `insertedAt` of missing records

```sql
SELECT insertedAt, startTime, req_id, objectKey
FROM logs.access_logs_federated
WHERE bucketName = '<affected-bucket>'
  AND raftSessionID = <id>
  AND objectKey IN ('<missing-obj-1>', '<missing-obj-2>', ...)
ORDER BY insertedAt, startTime, req_id
```

This is the critical query that Runs 1 and 2 could not execute because TTL had expired.

#### 1d. Cross-reference delivered vs missing by `insertedAt` second

Using the delivered object keys (from S3 log objects) and the full set (from ClickHouse):

```sql
SELECT
    insertedAt,
    count() AS total,
    countIf(objectKey NOT IN ('<missing-obj-1>', '<missing-obj-2>', ...)) AS delivered,
    countIf(objectKey IN ('<missing-obj-1>', '<missing-obj-2>', ...)) AS missing
FROM logs.access_logs_federated
WHERE bucketName = '<affected-bucket>'
  AND raftSessionID = <id>
GROUP BY insertedAt
ORDER BY insertedAt
```

#### 1e. Evaluate the result

**Hypothesis confirmed if:** The missing records cluster in one or more `insertedAt` seconds that are 100% missing, while surrounding `insertedAt` seconds are 100% delivered. This proves log-courier never saw those `insertedAt` seconds — it jumped over them because later-inserted data was visible first.

**Hypothesis refuted if:** The missing records share the same `insertedAt` seconds as delivered records (i.e., partial loss within an `insertedAt` second). This would point to a different mechanism — likely within-second ordering or LIMIT interaction under conditions not yet reproduced.

## Test Run 3 (2026-03-18)

**Bucket prefix:** `20260318090028`
**Investigation date:** 2026-03-18
**Image:** `1.0.3-processing-delay-debug` (with 60s processing delay)

### Result

- **Expected:** 3,000,000 PutObject log entries delivered
- **Missing:** 2,870
- **Duplicates:** 0
- **Verdict:** FAILED — processing delay did not prevent data loss

### Missing entries by bucket

| Bucket | Missing | Gaps |
|--------|---------|------|
| bucket-3 | 1,426 | obj-121001–121715 (715), obj-122425–123135 (711) |
| bucket-15 | 1,444 | obj-123726–124446 (721), obj-125172–125894 (723) |

Same pattern as Runs 1 and 2: two contiguous ranges of missed objects per bucket, with a gap of ~700 delivered objects between them.

### Proven Facts (Run 3)

#### 19. Processing delay (60s) does not prevent data loss

Log-courier ran with `consumer.processing-delay-seconds: 60`, which adds `AND insertedAt < now() - INTERVAL 60 SECOND` to both BatchFinder and LogFetcher queries. Despite excluding records inserted within the last 60 seconds, 2,870 records were still missed.

#### 20. All missing records exist in ClickHouse

All 150,002 records per affected bucket (150,000 PutObject + 2 bucket creation) are present in ClickHouse. The missing records were ingested but not delivered.

#### 21. The committed offset is past all records

The offsets table shows the final committed offset reached the maximum `insertedAt` for each bucket:

| Bucket | raftSessionID | lastProcessedInsertedAt | lastProcessedStartTime |
|--------|---------------|-------------------------|------------------------|
| bucket-3 | 4 | 2026-03-18 10:12:08 | 2026-03-18 10:12:00.265 |
| bucket-15 | 8 | 2026-03-18 10:12:08 | 2026-03-18 10:12:00.250 |

Zero records exist beyond the committed offset. Log-courier believes it processed everything.

#### 22. Missing records are at `insertedAt=09:13:08`, delivered gap records are at `insertedAt=09:13:07`

For bucket-3, the row positions (in fetch order: `insertedAt ASC, startTime ASC, req_id ASC`) and `insertedAt` values of the boundary objects:

| Object key | Row position | insertedAt | startTime | Status |
|-----------|-------------|------------|-----------|--------|
| obj-121716 (gap start) | 121,003 | 09:13:07 | 09:13:01.081 | Delivered |
| obj-122424 (gap end) | 121,711 | 09:13:07 | 09:13:04.380 | Delivered |
| obj-121001 (missed start) | 121,712 | 09:13:08 | 09:12:57.761 | Missing |
| obj-121715 (missed end) | 122,426 | 09:13:08 | 09:13:01.077 | Missing |
| obj-122425 (missed start) | 122,427 | 09:13:08 | 09:13:04.385 | Missing |
| obj-123135 (missed end) | 123,137 | 09:13:08 | 09:13:07.682 | Missing |

The delivered "gap" records at `insertedAt=09:13:07` have startTime range `09:13:01–09:13:04`. The missed records at `insertedAt=09:13:08` have startTime range `09:12:57–09:13:07` — their startTime values *overlap with and extend below* the delivered records' startTime values.

#### 23. `startTime` at `insertedAt=09:13:08` spans 10 seconds and starts BEFORE `insertedAt=09:13:07`

| insertedAt | Records | startTime min | startTime max |
|------------|---------|---------------|---------------|
| 09:13:07 | 709 | 09:13:01.081 | 09:13:04.380 |
| 09:13:08 | 1,443 | 09:12:57.761 | 09:13:07.756 |

Records inserted at second `09:13:08` contain requests that started as early as `09:12:57` — 11 seconds *before* the records inserted at `09:13:07` which have requests starting at `09:13:01`. The `startTime` ordering within an `insertedAt` second is inverted relative to the adjacent second.

#### 24. The `insertedAt` distribution shows 133 distinct seconds across a 69-minute window

Bucket-3 has 150,002 records spanning `insertedAt` from `09:03:37` to `10:12:08`, distributed across 133 distinct seconds with 1–2,849 records per second.

### Evaluation of Testable Prediction

The prediction from the out-of-order visibility hypothesis stated:

> *"The missing records will have `insertedAt` values strictly less than the `insertedAt` of records delivered around the same time — entire `insertedAt` seconds will be 100% missing."*

**Result: Partially confirmed, partially refuted.**

The missing records are concentrated at `insertedAt=09:13:08` (1,443 records at that second). The delivered gap records are at a different second (`insertedAt=09:13:07`, 709 records). This is consistent with out-of-order visibility: records at `09:13:08` arrived late and were skipped when the offset had already advanced past them.

However, `insertedAt=09:13:08` is not *strictly less than* the surrounding delivered seconds — it is *greater*. And it is not clear whether ALL records at that second are missing or just a subset. The prediction expected the missing records to have *earlier* `insertedAt` values, but they have a *later* `insertedAt` value with *earlier* `startTime` values.

This suggests the loss mechanism involves the interaction between `insertedAt` ordering and `startTime` ordering within the composite offset, not just entire seconds being skipped by out-of-order visibility.

## Summary

| Question | Answer |
|----------|--------|
| Where were logs lost? | After ClickHouse ingestion, before S3 delivery |
| Which component? | Log-courier (confirmed in run 2, fact #15) |
| How many? | Run 1: 708. Run 2: 3,974. Run 3: 2,870 |
| Pattern? | Contiguous gaps in the middle of the data stream |
| Duplicates? | 0 in all runs |
| Reproducible? | Yes — same gap pattern across three independent test runs |
| LIMIT + offset skip? | Ruled out for Runs 1–2 config (fact #16). Run 3 has 150k records/bucket (LIMIT activates), but missed records are within a single batch, not at a LIMIT boundary |
| Processing delay fix? | No — 60s delay did not prevent loss (fact #19) |
| Root cause proven? | Partially — loss confirmed in the offset mechanism. Missing records have `insertedAt` one second later than delivered neighbors, but with earlier `startTime` values (facts #22, #23). Exact skip mechanism still under investigation |
| `rotate_wait` timeout? | Ruled out — missing entries are mid-file, not at file tail |
| Leading hypothesis | Composite offset `(insertedAt, startTime, reqID)` skips records when `startTime` ordering is inverted relative to `insertedAt` ordering across adjacent seconds |
| Key next step | Determine exact batch boundary and offset state at the time the missed records were skipped |
