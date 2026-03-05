# Notebook Review — Iceberg Novice Perspective

Reviewed as a newcomer with MySQL/Hive experience, no prior Iceberg knowledge.

---

## E1.1 - OpenLakehouse

### Cell 9 (code) — Missing bridge to familiar concepts
> `USING iceberg`
**Question**: I've never seen `USING <format>` in MySQL or Postgres. In Hive I'd write `STORED AS PARQUET` to pick a file format — is `USING iceberg` the Spark equivalent? What happens if I leave it off — do I get a non-Iceberg table?
**Suggestion**: A one-line note like "USING iceberg tells Spark to create an Iceberg table rather than a plain Spark-managed table" would orient me immediately.

### Cell 9 (code) — New term without definition
> `'format-version' = '3'`
**Question**: The comment says V3 enables "initial column defaults" and that V1/V2 are in wide use, but what do V1 and V2 actually provide? I don't know what I'd be missing by choosing V2, or why there's a one-way upgrade.
**Suggestion**: A brief list of what each version unlocked (V1: base spec, V2: row-level deletes, V3: default values) would help me understand the progression without needing to look it up.

### Cell 19 (markdown) — New term without definition
> "Click into the `data/` folder to see the actual Parquet data files"
**Question**: What is Parquet? I've worked with MySQL row stores and CSV flat files. Is Parquet a binary columnar format? Why does Iceberg use it instead of something else?
**Suggestion**: One sentence like "Parquet is a columnar binary file format widely used in big data — think of it as a compressed, column-oriented alternative to CSV" would bridge the gap for someone coming from a relational background.

---

## E1.2 - DataModeling

### Cell 15 (code) — New term without definition
> `readable_metrics.total_amount.lower_bound`
**Question**: What is `readable_metrics`? I see it used as a nested column in the `files` metadata table, but it was never introduced. Is this something Iceberg generates automatically for every column? What other fields does it have besides `lower_bound` and `upper_bound`?
**Suggestion**: Before the first use, a brief note explaining that `readable_metrics` is an Iceberg metadata column containing per-file statistics (min, max, null count, etc.) for every data column would help me understand what I'm looking at.

### Cell 16 (code) — New term without definition
> `FROM polaris.taxi.trips_unpartitioned.entries`
**Question**: We just learned about `.files` and `.snapshots` metadata tables, but `.entries` appeared without introduction. How is it different from `.files`? The comment says "it parallelizes better on Spark" — but why? And what does it return that `.files` doesn't?
**Suggestion**: A one-liner introducing `.entries` (e.g., "The entries table exposes manifest-level entries and parallelizes better in Spark than the files table") before its first use would prevent the "wait, where did this come from?" reaction.

### Cell 58 (markdown) — Broken flow
> "Monthly + Sorted doesn't benefit because of lack of predicate on sort column"
**Question**: But the Monthly + Sorted table was actually the fastest at 0.110s — faster than both Monthly (0.143s) and Daily (0.161s). If the sort order doesn't benefit here, why did it win? The analysis text seems to contradict the numbers, and I'm not sure what to take away.
**Suggestion**: Clarify that the Monthly + Sorted table still benefits from monthly partition pruning (same as Monthly), and the slight time difference is likely noise or from having a slightly different file layout. The key point — that the sort order specifically adds no benefit without a predicate on the sort column — could be stated more precisely.

### Cell 32 (markdown) — Missing "why"
> "While picking out a partitioning spec remember that query cost scales with the number of files and the amount of data being read."
**Question**: I understand that small files are bad, but how small is too small? The note about "100MB - 1GB per partition" appears much later in the summary. Having a rough guideline earlier would help me judge the daily partition output (1.8MB per file) as I'm looking at it.
**Suggestion**: Mention the target file size range (or at least "files under a few MB start to hurt") closer to where the daily partitioning results are shown.

---

## E2.1 - MovingExistingTables

### Cell 31 (code) — Assumed Iceberg knowledge
> `spark.sql("USE spark_catalog")`
**Question**: What is `spark_catalog`? The notebook switches to it because "the Polaris catalog only manages Iceberg tables," but I didn't know Spark had its own built-in catalog running alongside Polaris. Can I have multiple catalogs active at once? How does Spark decide which one to use?
**Suggestion**: A brief note before the switch — something like "Spark always has a built-in catalog called `spark_catalog` for non-Iceberg tables. You can have multiple catalogs configured simultaneously and switch between them with `USE`" — would make this pattern feel less surprising.

### Cell 43 (code) — Missing bridge to familiar concepts
> `source_table => 'parquet.\`s3a://...\`'`
**Question**: The backtick-quoted path inside `parquet.\`...\`` is unusual syntax. Is this a Spark convention for referencing raw files as if they were tables? I've never seen this in MySQL or standard SQL. Where can I use this pattern — only in procedures, or in regular queries too?
**Suggestion**: A short note explaining that `parquet.\`<path>\`` is Spark's way of reading raw Parquet files inline (and that we already used it in earlier CTAS statements) would connect the dots.

### Cell 52 (code) — New term without definition
> `summary['added-records']`
**Question**: The `summary` column in the snapshots table seems to be a map with useful keys like `added-records` and `deleted-records`. But it was never introduced — what other keys does it contain? Is this the same across all Iceberg engines?
**Suggestion**: When the `summary` map is first used, a note listing a few common keys (`added-records`, `deleted-records`, `added-data-files`, `total-records`) would help me know what's available to query.

---

## E2.2 - BranchingAndTagging

### Cell 33 (code) — New term without definition
> `FROM polaris.timetravel.nyc_taxi.refs`
**Question**: What is the `refs` metadata table? The name "refs" isn't self-explanatory — is it short for "references"? It shows branches and tags, but I wouldn't have known to look here without being told.
**Suggestion**: A sentence before the query like "The `refs` metadata table lists all named references (branches and tags) and the snapshot each one points to" would set the context.

### Cell 39 (markdown) — Missing "why"
> "You can jump to *any* snapshot — forward or backward — using `set_current_snapshot`."
**Question**: We already rolled back using `rollback_to_snapshot` a few cells earlier, and now we're using `set_current_snapshot` to do something similar. What's the actual difference between these two procedures? When would I choose one over the other?
**Suggestion**: A brief comparison — e.g., "`rollback_to_snapshot` only moves backward and is the safe choice for undoing mistakes; `set_current_snapshot` can jump to any snapshot (forward or backward) and is useful for navigation" — would clear up the redundancy.

### Cell 65 (code) — New term without definition
> `INSERT INTO polaris.timetravel.nyc_taxi.branch_august_load`
**Question**: The `.branch_<name>` syntax for addressing a branch appeared without introduction. Is this a Spark-specific convention? What happens if my branch name has special characters? And is this the only way to write to a branch, or can I `USE` a branch the way I switch catalogs?
**Suggestion**: Before the first branch write, explain the naming convention: "In Spark, you address a branch by appending `.branch_<name>` to the table name."

---

## E2.3 - SchemaAndPartitionEvolution

### Cell 20 (markdown) — New term without definition
> "V2 added row-level delete support"
**Question**: What does "row-level delete support" mean in a file-based table format? In MySQL, deleting a row is straightforward — you mark it as deleted. But Iceberg stores data in immutable files, so how does V2 handle deletes differently from V1? Is this related to the COW/MOR distinction mentioned briefly in E2.2?
**Suggestion**: A parenthetical like "(the ability to delete or update individual rows within files, rather than only appending or dropping entire files — we'll explore how this works in E3.1)" would satisfy my curiosity without requiring a full explanation here.

### Cell 56 (markdown) — Broken flow
> "Slot 1 = day (from spec 1), Slot 2 = month (from spec 2) ... The tuple grows each time a new partition field is added"
**Question**: The unified partition tuple concept is powerful but the explanation is dense. I had to read it three times. The paragraph first says "Before the second evolution, the tuple has only one slot" but the visual mapping already shows two slots. Also, values like `{NULL, 643}` require me to hold multiple concepts (tuple slots, month offsets from epoch, spec history) in my head simultaneously.
**Suggestion**: Consider walking through the tuple evolution step-by-step: "After spec 1 is added, the tuple has one slot: `{day}`. After spec 2 is added, a second slot appears: `{day, month}`." Building it incrementally rather than presenting the final state first would be easier to follow.

### Cell 66 (markdown) — New term without definition
> "`bucket(N, col)`, and `truncate(N, col)`"
**Question**: These transforms are mentioned as options for the Try It exercise, but I have no idea what they do. `bucket` sounds like it might hash values into N groups (like hash partitioning in MySQL), and `truncate` might chop strings or numbers, but I'm guessing. How do I choose between them and the temporal transforms?
**Suggestion**: One sentence each: "bucket(N, col) hashes column values into N fixed groups — similar to hash partitioning in MySQL. truncate(N, col) groups values by their first N characters (for strings) or by rounding to the nearest N (for numbers)."

---

## E3.1 - Ingestion

### Cell 8 (markdown) — Missing "why"
> "Large infrequent writes. Typical for daily ETL jobs."
**Question**: The section jumps straight into batch ingestion without explaining what makes a write "batch" vs "streaming" in Iceberg terms. From my traditional database background, all writes are just inserts — the database handles them the same way. Why does the distinction matter for Iceberg? Is it because each write creates a snapshot, and more frequent writes mean more snapshots and more small files?
**Suggestion**: A brief intro paragraph before the two patterns: "In traditional databases, the engine manages write frequency transparently. In Iceberg, every commit creates a new snapshot and potentially new data files, so the frequency and size of writes directly affect metadata accumulation and file layout."

### Cell 28 (code) — Missing "why"
> `00001-1162-...-00001-deletes.parquet`
**Question**: The MOR table shows a file ending in `-deletes.parquet`. The markdown in Cell 29 says "the delete file marks which rows were changed," but what does it actually contain? Is it a list of row positions? Row IDs? A copy of the deleted rows? Understanding the physical structure would help me reason about the MOR read-merge overhead.
**Suggestion**: One sentence like "A delete file contains the positions (row numbers) of rows that have been changed or removed — at read time, Spark skips those positions in the original data file and reads the replacement rows from the new data file" would ground the concept.

### Cell 17 (markdown) — Missing bridge to familiar concepts
> "Each snapshot adds metadata overhead — more manifests, more small files to manage."
**Question**: In MySQL, I can do thousands of small inserts per second and the database handles compaction internally. Why does Iceberg care about write frequency? The connection between "commit = snapshot = new manifest + new files" isn't explicit enough for me to understand why streaming is more expensive than batch for metadata.
**Suggestion**: Add a sentence bridging to the familiar: "Unlike MySQL's InnoDB which merges small writes in the background, Iceberg commits are immutable — each one writes new files to object storage. The maintenance procedures in E3.2 serve the role that autovacuum plays in Postgres."

---

## E3.2 - MaintenanceProcedures

### Cell 8 (code) — New term without definition
> `'commit.manifest-merge.enabled' = 'false'`
**Question**: This property is set to `false` to intentionally create fragmentation, but I didn't know automatic manifest merging existed in the first place. Is Iceberg normally merging manifests on every commit? What does that look like? How aggressive is it?
**Suggestion**: Before disabling it, explain what you're turning off: "By default, Iceberg merges small manifests into larger ones during each commit (controlled by `commit.manifest-merge.enabled`). We disable this to simulate the worst case."

### Cell 50 (code) — Missing bridge to familiar concepts
> `older_than => TIMESTAMP '2099-12-31 23:59:59', retain_last => 1`
**Question**: Setting `older_than` to 2099 feels like a workaround. The comment says `expire_snapshots` applies both conditions, but I find the interaction confusing — does it mean "expire snapshots that are both older than X AND beyond the last N"? In Postgres, `VACUUM` doesn't require this kind of parameter gymnastics.
**Suggestion**: Clarify the semantics: "expire_snapshots keeps the most recent `retain_last` snapshots regardless of age, then expires everything else that is older than `older_than`. Setting `older_than` far in the future ensures the age condition doesn't protect any snapshot, so only `retain_last` matters." A brief note that production usage typically relies on `older_than` with a reasonable retention window (e.g., 7 days) would help too.

### Cell 38 (code) — Broken flow
> Partition output shows only `{2023-05-31}`, `{2023-06-01}`, `{2023-06-02}`
**Question**: We loaded June data, but the partition output shows only 3 partitions. Earlier notebooks showed 30+ days for June. Is this because of the `LIMIT` used in the micro-batches? If so, the output is misleading — it looks like the table only covers 3 days, which makes the compaction results harder to interpret.
**Suggestion**: Either note that the micro-batch pattern (using `.limit()`) pulls from the head of the dataframe and may not cover all days, or adjust the batching to sample across the full date range.

---

## E3.3 - TableModelingAndIngestion

### Cell 43 (code) — New term without definition
> `PARTITIONED BY (bucket(16, PULocationID))`
**Question**: The `bucket` transform is used as the primary partitioning strategy for a major experiment, but it was never explained — only listed in a Try It section in E2.3. What does `bucket(16, ...)` do? Does it hash PULocationID into 16 groups? How do I choose 16 vs 32 vs 64? Is there a performance tradeoff in the number of buckets?
**Suggestion**: Before the first use in an experiment, add a brief definition: "The `bucket(N, col)` transform hashes column values into N fixed groups. It's useful for columns with high cardinality where temporal transforms don't apply. Choose N based on your target file count — more buckets means more files but finer-grained pruning."

### Cell 45 (markdown) — Assumed Iceberg knowledge
> "Range distribution performs a more sophisticated shuffle: it samples the data to determine range boundaries"
**Question**: What does "sampling" mean here? Does Spark read a fraction of the data to estimate value distributions? How does this affect write cost compared to hash? The term is used casually but I don't have enough context to understand the mechanism.
**Suggestion**: A brief parenthetical: "(Spark reads a small sample of the data to estimate value distributions and determine partition boundaries, which adds a small overhead compared to hash)" would make the mechanism concrete.

### Cell 67 (code) — New term without definition
> `strategy => 'sort'`
**Question**: In E3.2, we used `rewrite_data_files` without a `strategy` parameter. Now there's `strategy => 'sort'`. What was the default strategy in E3.2? What other strategies exist? Does `'sort'` respect the table's configured sort order, or do I need to specify the sort key separately?
**Suggestion**: A note like "`strategy => 'sort'` tells the compaction to re-sort files according to the table's configured sort order. The default strategy ('binpack') only combines small files without re-sorting — faster but doesn't restore sort order benefits." This would also retroactively clarify what E3.2's compaction was doing.

### Cell 62 (markdown) — Broken flow
> "hash and none produce nearly identical results"
**Question**: But looking at the numbers, none produced 200 files (1.6 per partition) vs hash's 129 files (1.0 per partition). That's 55% more files. The text says "nearly identical" but the file counts aren't close. Is the point that the file *contents* are the same quality, even if there are more files? Or that query performance is similar despite the extra files?
**Suggestion**: Acknowledge the file count difference explicitly: "None produces more files than hash because boundary partitions get split across Spark tasks. But the file contents are well-partitioned — no partition mixing — so query performance is similar."

---
---

# Try It Exercise Review — Hands-On Attempt

Each "Try It" exercise was attempted by following only the scaffold code and instructions, running outside the notebooks to preserve cell state. 21 exercises were attempted; 19 succeeded, 2 failed due to scaffold issues.

## Summary

| Result | Exercise | Notes |
|--------|----------|-------|
| PASS | E1.2 — Try Your Own Query | Clear, but requires notebook-defined `compare_partitioning_strategies()` |
| PASS | E2.1 — Migrate Your Own CSV | Worked smoothly |
| **FAIL** | **E2.1 — Inspect the Metadata** | **Scaffold uses wrong column name** |
| PASS | E2.1 — Migrate Another Month | Worked after filling in `???` |
| PASS | E2.2 — Time Travel Query | Straightforward |
| PASS | E2.2 — Tag and Rollback | Worked, scaffold well-structured |
| PASS | E2.2 — WAP Write | Worked, scaffold well-structured |
| PASS | E2.2 — Create Branch | Worked, scaffold well-structured |
| PASS | E2.3 — Evolve the Schema | Straightforward |
| PASS | E2.3 — Data Resurrection Prevention | Requires `show_iceberg_schema()` helper from notebook |
| PASS | E2.3 — Rename and Verify | Straightforward |
| PASS | E2.3 — Partition Transform | Requires `show_partition_specs()` helper from notebook |
| PASS | E3.1 — Streaming Params | Scaffold clear, requires notebook helper |
| PASS | E3.1 — COW vs MOR Selectivity | Scaffold very clear with suggested values table |
| **FAIL** | **E3.1 — Write Your Own MERGE** | **Scaffold leads to MERGE_CARDINALITY_VIOLATION** |
| PASS | E3.1 — Non-Conflicting Ranges | Excellent exercise; scaffold + metrics output made it solvable |
| PASS | E3.2 — Compact with Different Target Size | Straightforward |
| PASS | E3.2 — Full Maintenance Sequence | Worked, scaffold well-structured |
| PASS | E3.3 — Different Query Pattern | Tables exist only during live execution |
| PASS | E3.3 — Different Sort Key | Self-contained scaffold, worked well |
| PASS | E3.3 — Combine Strategies | Self-contained scaffold, worked well |

---

## Detailed Findings

### E2.1 Cell 36 (code) — Try It: Inspect the Metadata — SCAFFOLD BUG

> `spark.sql(f"SELECT path, added_files_count FROM {snapshotted_table}.manifests")`

**What happened**: The scaffold suggests querying `added_files_count` from the `manifests` metadata table. This column does not exist. The actual column name is `added_data_files_count`. Running the scaffold code produces:

```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter
with name `added_files_count` cannot be resolved. Did you mean one of the
following? [`added_data_files_count`, `added_delete_files_count`,
`deleted_data_files_count`, `added_snapshot_id`, `deleted_delete_files_count`]
```

**Impact**: A novice would get a confusing error on what should be a simple exploration exercise. The error message does suggest the correct column name, but it undermines confidence early in the learning process.
**Fix**: Change `added_files_count` to `added_data_files_count` in the scaffold comment.

---

### E3.1 Cell 53 (code) — Try It: Write Your Own MERGE — SCAFFOLD LEADS TO ERROR

> ```
> MERGE INTO polaris.ingestion.taxi_corrections t
> USING my_corrections s
> ON t.tpep_pickup_datetime = s.tpep_pickup_datetime
>    AND t.VendorID = s.VendorID
>    AND t.trip_distance = s.trip_distance
> WHEN MATCHED THEN UPDATE SET ???
> WHEN NOT MATCHED THEN INSERT *
> ```

**What happened**: Following the scaffold's pattern — creating a corrections view from the first 50 rows and merging on `(tpep_pickup_datetime, VendorID, trip_distance)` — produces a `MERGE_CARDINALITY_VIOLATION`:

```
The ON search condition of the MERGE statement matched a single row from the
target table with multiple rows of the source table.
```

This happens because the ON clause columns (`tpep_pickup_datetime`, `VendorID`, `trip_distance`) do not uniquely identify rows in the taxi dataset. Multiple trips can share the same vendor, pickup time, and distance. The scaffold reuses the same ON clause from the example MERGE (Cell 50) which worked there because the first 100 rows happened to be unique on those columns — but a novice modifying the LIMIT or columns is likely to hit this.

**Impact**: A novice following the instructions step-by-step will get an error they don't understand. The concept of MERGE cardinality requirements isn't explained, and there's no guidance on how to pick a unique key or handle duplicates.
**Suggestion**: Either (a) warn that the ON clause must uniquely match rows and suggest adding more columns if needed, or (b) use `LIMIT 50` on a pre-deduplicated view, or (c) add a note about what `MERGE_CARDINALITY_VIOLATION` means and how to fix it.

---

### E1.2 Cell 66 (code) — Try It: Try Your Own Query — HELPER NOT ACCESSIBLE

> `compare_partitioning_strategies(where_clause, description)`

**What happened**: The scaffold tells the novice to call `compare_partitioning_strategies()` which is defined in Cell 51 of the notebook. This works fine when running inside the notebook, but a novice who tries to write the code from scratch (or in a separate notebook) would need to either copy the function or write their own loop over the four tables.

**Impact**: Mild. The function is accessible in the notebook context and the instructions are clear. But the scaffold doesn't give the novice practice writing their own query loop — they just call a pre-built function.
**Suggestion**: Consider showing the raw query pattern (looping over tables) as an alternative, so the novice learns the pattern rather than just calling a helper.

---

### E2.3 Cell 67 (code) — Try It: Partition Transform — HELPER NOT ACCESSIBLE

> `show_partition_specs("polaris.evolution.taxi_evolving")`

**What happened**: The scaffold calls `show_partition_specs()` which is defined via the Java py4j bridge in Cell 6. The novice has no way to inspect partition specs without this helper — `DESCRIBE TABLE` doesn't show specs, and there's no SQL-accessible metadata table for partition specs.

**Impact**: The exercise works in-notebook but a novice who wants to verify their partition transform change independently (e.g., "did my `hours()` partition actually get added?") has no path forward without the helper. They'd have to trust the INSERT worked.
**Suggestion**: Mention that the novice can also verify the partition change by inspecting the `files` metadata table — if the new files have the expected partition values, the transform is working.

---

### E2.1 Cell 20 (code) — Try It: Migrate Your Own CSV — MIGRATION STEP UNCLEAR

> `# Migrate to Iceberg using CTAS`
> `# ???`

**What happened**: The scaffold has `???` for the actual migration command with the comment "Migrate to Iceberg using CTAS." The notebook showed two different patterns earlier: `spark.sql("CREATE TABLE ... AS SELECT ...")` (Cell 13) and `df.writeTo(...).using("iceberg").create()` (also Cell 13). With two patterns shown, I wasn't sure which one the scaffold expected, and the `???` gives no structural hint (is it a `spark.sql()` call? a `writeTo()`?).

**Impact**: A confident novice can figure it out from the examples above, but the lack of any structural hint (even an empty `spark.sql(f"...")` or `my_df.writeTo(...)`) means the novice is writing from memory rather than filling in blanks.
**Suggestion**: Replace `???` with a partial statement like `my_df.writeTo(my_table_name).???` or `spark.sql(f"CREATE TABLE {my_table_name} ...")` to give a structural starting point.

---

### E2.2 Cell 44 (code) — Try It: Tag and Rollback — `WHERE name = ???` QUOTING AMBIGUOUS

> `WHERE name = ???`

**What happened**: The scaffold has `WHERE name = ???` without indicating whether the placeholder should be a string literal (quoted) or a variable reference. A novice might try `WHERE name = my_tag_name` (no quotes, Python variable) or `WHERE name = 'my_tag_name'` (string literal). Since this is inside an f-string with `spark.sql()`, the correct form is `WHERE name = '{my_tag_name}'` — but the scaffold doesn't make this clear.

**Impact**: Minor friction. Most novices would figure it out, but the quoting question adds unnecessary cognitive load during what should be a confidence-building exercise.
**Suggestion**: Show the placeholder with quotes: `WHERE name = '???'`

---

### E3.2 Cell 62 (code) — Try It: Full Maintenance Sequence — EXPIRE SNAPSHOTS PARAMETER

> `spark.sql(f"CALL polaris.system.expire_snapshots(table => '{my_table.split('.', 1)[1]}', retain_last => 2)")`

**What happened**: The scaffold expires snapshots with `retain_last => 2` but doesn't include `older_than`. Based on the lesson in Cell 50 (which explained that `expire_snapshots` applies both conditions), I expected to need `older_than` as well. Running without it actually works — `retain_last` alone is sufficient when you don't care about the age condition. But the lesson created the impression that both parameters are always needed together.

**Impact**: The novice might hesitate, wondering whether they need the `older_than` parameter too. The exercise itself works, but it contradicts the mental model built in Part 4.
**Suggestion**: In Cell 50's explanation, note that `retain_last` works on its own — `older_than` defaults to a sensible value when omitted.

---

### Overall Try It Assessment

**Strengths:**
- Most exercises are well-scaffolded with clear comments and `???` placeholders
- The E2.2 exercises (time travel, tag, WAP, branch) are consistently excellent — the scaffolds mirror the demonstrated patterns closely
- E3.1's COW vs MOR selectivity exercise has a particularly good scaffold with a table of suggested parameter values and expected behaviors
- E3.3's exercises are self-contained (they create and drop their own tables), which prevents dependency issues

**Areas for improvement:**
- Two scaffolds have bugs (wrong column name in E2.1, cardinality trap in E3.1)
- Several exercises depend on notebook-defined helper functions (`compare_partitioning_strategies`, `show_partition_specs`, `run_streaming_ingest`, `benchmark_cow_vs_mor`). This is fine for in-notebook use but means the novice is calling a function rather than learning the underlying pattern
- The `???` placeholder style is inconsistent — some give structural hints (e.g., `table => ???`), others give nothing (just `# ???` on its own line)
- No exercise explicitly warns about common errors the novice might encounter (e.g., MERGE cardinality, column name mismatches in metadata tables)
