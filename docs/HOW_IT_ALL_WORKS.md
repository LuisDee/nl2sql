# How the NL2SQL Agent Works — From Trader Question to Answer

A complete explanation of every step, written for humans who are still getting their head around embeddings, vectors, and semantic search.

---

## Part 1: The Problem We're Solving

A trader types: **"what was the average edge on market trades today?"**

The agent needs to:
1. Figure out which of our 12 BigQuery tables to query (there are 5 KPI tables and 7 data tables)
2. Know what columns exist in that table and what they're called (e.g. the trader says "edge" but the column is called `edge_bps`)
3. Write correct SQL
4. Validate it
5. Run it
6. Return the answer

The hard part is step 1 and 2. We can't use simple keyword matching because traders say things in dozens of different ways:
- "what was the edge?" / "how much edge did we capture?" / "trading edge today" — all mean the same thing
- "implied vol" / "IV" / "sigma" / "volatility" — all refer to the `vol` column in `theodata`

We need **semantic understanding** — matching by meaning, not by exact words.

---

## Part 2: What Are Embeddings? (The GPS Analogy)

Think of embeddings as **GPS coordinates for meaning**.

London and Paris are geographically close, so their GPS coordinates are similar numbers. Tokyo is far away, so its coordinates are very different numbers.

Embeddings work the same way, but for text:

| Text | Embedding (simplified to 3 numbers) |
|------|--------------------------------------|
| "what was the edge on our trades?" | `[0.82, 0.15, -0.34]` |
| "how much edge did we capture today?" | `[0.80, 0.17, -0.31]` |
| "show me the order book depth" | `[-0.12, 0.65, 0.43]` |

The first two are about "edge" so their numbers are very close together. The third is about something completely different, so its numbers are far away.

In reality, our embeddings have **768 numbers** instead of 3 (the model `text-embedding-005` outputs 768-dimensional vectors). More dimensions = more nuance about meaning.

### How Do You Measure "Close"?

**Cosine distance.** It measures the angle between two arrows (vectors).

- Two arrows pointing in the same direction → cosine distance = **0** (identical meaning)
- Two arrows at 90 degrees → cosine distance = **1** (unrelated)
- The smaller the number, the more similar the meaning

So when the agent gets distance = 0.12 for `markettrade` and distance = 0.85 for `marketdepth`, it knows markettrade is the right table for an "edge" question.

---

## Part 3: Our Three Embedding Tables — What They Are and Why

We have three tables in BigQuery that store pre-computed embeddings. Each serves a different purpose in the agent's decision-making.

### Table 1: `schema_embeddings` (~17 rows)

**What's in it:** One row per table, one per dataset, and a few "routing hint" rows. Each row has a **text description** of what that table contains, plus the **embedding** (768 numbers) of that description.

**Why we have it:** This is how the agent decides which table to query. It's the "table of contents" for our data.

**What a row looks like:**

```
Row 1:
  source_type:  "table"
  layer:        "kpi"
  dataset_name: "nl2sql_omx_kpi"
  table_name:   "markettrade"
  description:  "KPI metrics for market exchange trades on OMX options.
                 One row per trade. Contains edge (difference between
                 machine fair value and trade price), instant_pnl,
                 delta_slippage at 1s/1m/5m/30m/1h/eod intervals.
                 Default KPI table when trade type is not specified."
  embedding:    [0.012, -0.034, 0.078, ... 768 numbers total]

Row 2:
  source_type:  "table"
  layer:        "data"
  dataset_name: "nl2sql_omx_data"
  table_name:   "theodata"
  description:  "Theoretical options pricing snapshots. Contains tv
                 (fair value), delta, vol (implied volatility), vega,
                 gamma, theta. ONLY exists in data dataset. Use for
                 any question about vol, IV, greeks, fair value."
  embedding:    [0.045, -0.011, 0.092, ... 768 numbers total]

Row 3:
  source_type:  "routing"
  layer:        NULL
  dataset_name: NULL
  table_name:   NULL
  description:  "Questions about theoretical pricing, implied volatility
                 IV vol sigma, delta, vega should route to theodata.
                 This table only exists in the data dataset."
  embedding:    [0.031, -0.019, 0.067, ... 768 numbers total]
```

**How the embedding was created:** We took each description and ran it through Google's `text-embedding-005` model with `task_type = RETRIEVAL_DOCUMENT` (meaning "this is a document to be searched"). The model returned 768 numbers that capture the meaning of that description.

### Table 2: `column_embeddings` (~4,631 rows)

**What's in it:** One row per column per table. Every single column across all 12 tables gets its own row with a description and an embedding.

**Why there are so many:** Our KPI tables are wide. `markettrade` alone has 774 columns (all the slippage intervals at different time buckets, reference values, greeks, etc.). Across 12 tables, that's 4,631 columns total.

**Why we have it:** Future use. The plan is to let the agent search for specific columns by meaning — e.g. a trader says "sigma" and the agent finds the `vol` column via its synonym list. This isn't wired into the agent yet (planned for Track 06), but the data is there ready to go.

**What a row looks like:**

```
Row 1:
  dataset_name: "nl2sql_omx_kpi"
  table_name:   "markettrade"
  column_name:  "algo"
  column_type:  "STRING"
  description:  "Name of the trading algorithm or strategy that executed
                 this market trade. Common values: OST_MQ, OST_X, ZB_TAK."
  synonyms:     ["strategy", "algo name", "trading algo", "execution algo"]
  embedding:    [0.021, -0.055, 0.038, ... 768 numbers total]

Row 2:
  dataset_name: "nl2sql_omx_data"
  table_name:   "theodata"
  column_name:  "vol"
  column_type:  "FLOAT64"
  description:  "Annualised implied volatility as decimal"
  synonyms:     ["IV", "implied vol", "sigma"]
  embedding:    [0.033, -0.018, 0.072, ... 768 numbers total]
```

### Table 3: `query_memory` (52 rows and growing)

**What's in it:** Validated question-to-SQL pairs. Each row is a question a trader asked, the SQL that correctly answered it, and some metadata. The question text is embedded so we can search it by meaning.

**Why we have it:** This is the agent's "memory" of past successes. When a new question comes in, the agent searches for similar past questions and uses their SQL as templates. This is called **few-shot learning** — giving the LLM a few examples of correct answers before asking it to write its own.

**What a row looks like:**

```
Row 1:
  question:       "What was the total instant PnL for market trades today?"
  sql_query:      "SELECT ROUND(SUM(instant_pnl), 2) AS total_pnl,
                   COUNT(*) AS trade_count
                   FROM `project.nl2sql_omx_kpi.markettrade`
                   WHERE trade_date = '2026-02-17'"
  tables_used:    ["markettrade"]
  dataset:        "nl2sql_omx_kpi"
  complexity:     "simple"
  routing_signal: "generic PnL question -> default to markettrade"
  embedding:      [0.015, -0.042, 0.061, ... 768 numbers total]

Row 2:
  question:       "How did implied vol change over the last month?"
  sql_query:      "SELECT trade_date, symbol,
                   ROUND(AVG(vol), 4) AS avg_vol
                   FROM `project.nl2sql_omx_data.theodata`
                   WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                   GROUP BY trade_date, symbol
                   ORDER BY trade_date"
  tables_used:    ["theodata"]
  dataset:        "nl2sql_omx_data"
  complexity:     "medium"
  routing_signal: "implied vol -> theodata"
  embedding:      [0.041, -0.027, 0.088, ... 768 numbers total]
```

**How it grows:** When a trader confirms a query result is correct ("yes, that's what I wanted"), the agent calls `save_validated_query` to insert a new row. Next time a similar question comes in, it'll find this example.

---

## Part 4: The Full Flow — What Happens When a Trader Asks a Question

Let's trace through exactly what happens when a trader types:

> **"what was the average edge on market trades today?"**

### Step 0: Semantic Cache Check

**Tool:** `check_semantic_cache`

Before doing anything expensive, the agent checks if this exact (or nearly exact) question was answered before.

```
1. Take the question text: "what was the average edge on market trades today?"
2. Send it to BigQuery: ML.GENERATE_EMBEDDING converts it to 768 numbers
   (using task_type = RETRIEVAL_QUERY — "this is a search query")
3. VECTOR_SEARCH compares those 768 numbers against the `embedding` column
   in query_memory (52 rows)
4. Find the closest match and its cosine distance
```

If the closest match has distance <= 0.10 (very similar meaning), it's a **cache hit**. The agent skips straight to step 5 with the cached SQL. This saves ~5 seconds.

If no close match, it's a **cache miss**. Continue to step 1.

### Step 1: Find the Right Table

**Tool:** `vector_search_tables`

Now the agent needs to figure out: which of our 12 tables should I query?

```
1. Take the question: "what was the average edge on market trades today?"
2. Send it to BigQuery with our combined SQL query
3. Inside BigQuery, this happens:

   a) ML.GENERATE_EMBEDDING converts the question to 768 numbers
      (task_type = RETRIEVAL_QUERY)

   b) VECTOR_SEARCH compares those 768 numbers against every row in
      schema_embeddings (17 rows). For each row, it computes:

      cosine_distance(question_embedding, row_embedding) = ?

   c) Results come back sorted by distance:

      markettrade  (kpi)   → distance 0.12  ← closest match!
      quotertrade  (kpi)   → distance 0.28
      clicktrade   (kpi)   → distance 0.35
      markettrade  (data)  → distance 0.42
      theodata     (data)  → distance 0.71

   d) At the same time, the combined query ALSO searches query_memory
      for similar past questions (few-shot examples). These get cached
      in Python memory for step 3.
```

The agent now knows: **use `nl2sql_omx_kpi.markettrade`**.

Why did it pick KPI markettrade over data markettrade? Because the description for KPI markettrade mentions "edge", "instant_pnl", "performance metrics" — and the question asks about "average edge". The embedding for "edge" in the question is very close to the embedding for "edge" in the KPI markettrade description.

### Step 2: Load Column Metadata

**Tool:** `load_yaml_metadata`

The agent now loads the YAML catalog file for `kpi/markettrade.yaml`. This gives it:
- All 774 column names and their types
- Descriptions of what each column means
- **Synonyms** — so it knows "edge" maps to the column called `edge_bps`
- Business rules (e.g. "always filter on trade_date")

This is plain file I/O — no embeddings involved here. The YAML catalog is the detailed reference manual.

### Step 3: Find Similar Past Queries

**Tool:** `fetch_few_shot_examples`

Remember in step 1, the combined query already searched `query_memory` and cached the results. So this step is instant — it just reads from Python memory.

The agent gets back examples like:
```
Past question: "What was the total instant PnL for market trades today?"
Past SQL:      SELECT ROUND(SUM(instant_pnl), 2) FROM markettrade WHERE trade_date = ...
Distance:      0.15 (fairly similar — both about KPI metrics on markettrade)
```

The agent shows these to the LLM as "here's how similar questions were answered before — follow this pattern."

### Step 4: Generate SQL

The LLM now has everything it needs:
- **Which table:** `nl2sql_omx_kpi.markettrade` (from step 1)
- **What columns exist:** 774 columns with descriptions (from step 2)
- **What "edge" means:** the column `edge_bps`, synonym "edge" (from step 2)
- **Example SQL patterns:** past queries on similar topics (from step 3)
- **Rules:** always filter on trade_date, use ROUND(), add LIMIT, etc. (from system prompt)

It generates:
```sql
SELECT
  ROUND(AVG(edge_bps), 4) AS avg_edge
FROM `project.nl2sql_omx_kpi.markettrade`
WHERE trade_date = '2026-02-19'
```

### Step 5: Validate SQL

**Tool:** `dry_run_sql`

Before running the query for real, the agent sends it to BigQuery's dry-run mode. This checks:
- Is the SQL syntactically valid?
- Do the table and column names exist?
- Does the user have permission?
- How much data will it scan? (cost estimate)

If it fails (e.g. wrong column name), the agent reads the error, fixes the SQL, and retries. It gets up to 3 attempts before the **circuit breaker** kicks in and stops it.

### Step 6: Execute SQL

**Tool:** `execute_sql`

The validated query runs against BigQuery. Results come back:

```
avg_edge: 0.0234
```

The agent formats this into a human-readable answer:

> "The average edge on market trades today was **0.0234 bps**. This represents the mean difference between Mako's fair value and the trade execution price across all market trades on 2026-02-19."

### Step 7 (Optional): Save to Memory

If the trader confirms the answer is correct, the agent calls `save_validated_query`. This:
1. Inserts the question + SQL into `query_memory`
2. BigQuery generates an embedding for the question text
3. Next time someone asks a similar question, it'll appear as a few-shot example

This is the **learning loop** — the agent gets better over time.

---

## Part 5: Where the Embeddings Come From (The Pipeline)

The embedding tables don't populate themselves. Here's what the scripts do:

### Building Time (you run this once, then again when catalog changes)

```
YAML catalog files                    Example YAML files
(catalog/kpi/markettrade.yaml, etc.)  (examples/kpi_examples.yaml, etc.)
         |                                      |
         | populate_embeddings.py reads          | populate_embeddings.py reads
         | column descriptions + synonyms        | question + SQL pairs
         |                                      |
         v                                      v
   column_embeddings table              query_memory table
   (4,631 rows of text)                (52 rows of text)
   embedding column = NULL              embedding column = NULL
         |                                      |
         |  run_embeddings.py --step generate-embeddings
         |  sends each description to Google's text-embedding-005 model
         |  model returns 768 numbers per row
         |                                      |
         v                                      v
   column_embeddings table              query_memory table
   (4,631 rows with embeddings)        (52 rows with embeddings)
   embedding = [0.02, -0.03, ...]      embedding = [0.01, -0.04, ...]
```

`schema_embeddings` is populated by `run_embeddings.py --step populate-schema` with hardcoded table/dataset descriptions (since these are hand-written, not from YAML columns).

### Query Time (every time a trader asks a question)

```
Trader's question
"what was the average edge?"
         |
         v
ML.GENERATE_EMBEDDING
(task_type = RETRIEVAL_QUERY)
         |
         v
[0.82, 0.15, -0.34, ... 768 numbers]
         |
         v
VECTOR_SEARCH compares against
schema_embeddings (17 pre-computed embeddings)
+ query_memory (52 pre-computed embeddings)
         |
         v
Returns rows sorted by cosine distance
(lowest distance = closest meaning)
```

### Two Different Task Types

When embedding text, you tell the model what the text is for:

- **`RETRIEVAL_DOCUMENT`** — used when embedding the stored descriptions/questions (building time). Tells the model "this is content that will be searched."
- **`RETRIEVAL_QUERY`** — used when embedding the trader's question (query time). Tells the model "this is a search query looking for matching content."

The model optimises the embeddings slightly differently for each case, which improves search accuracy.

---

## Part 6: Why This Architecture?

### Why not just use keywords?

Keyword search would fail on:
- "what was the edge?" → doesn't contain the word "markettrade"
- "how did IV change?" → "IV" doesn't appear in any table name
- "BGC vs MGN performance" → these are broker names that only exist in brokertrade

Semantic search understands that "edge" relates to trading performance (→ KPI tables) and "IV" relates to implied volatility (→ theodata).

### Why not put everything in one table?

We split into three tables because they serve different purposes at different stages:

1. **schema_embeddings** (17 rows) — searched first, fast, answers "which table?"
2. **column_embeddings** (4,631 rows) — future column-level routing
3. **query_memory** (52+ rows, growing) — answers "how was a similar question answered before?"

Searching 17 rows is much faster than searching 4,631. And the agent doesn't need column-level detail just to pick the right table.

### Why cache the combined result?

Before Track 08, every question triggered **3 separate calls** to `ML.GENERATE_EMBEDDING`:
1. Semantic cache check (query_memory search)
2. Table search (schema_embeddings search)
3. Few-shot examples (query_memory search again)

Each call takes 1-2 seconds of Vertex AI latency. Now steps 2 and 3 share a single embedding call via a SQL CTE (Common Table Expression), and the results are cached in Python. This cuts the embedding calls from 3 to 1-2.

---

## Part 7: Safety Nets

### Before-tool guard
Every SQL query passes through `before_tool_guard` which blocks any INSERT/UPDATE/DELETE/DROP. The agent is read-only.

### Circuit breaker
If `dry_run_sql` fails 3 times in a row, the circuit breaker blocks further SQL tool calls entirely. The agent must stop and explain the error instead of looping forever.

### Tool call limit
A global counter limits the agent to 15 tool calls per turn. If something goes wrong and the agent starts calling tools in a loop, it gets cut off.

---

## Summary: The Whole Thing in One Picture

```
TRADER: "what was the average edge today?"
  |
  |  Step 0: check_semantic_cache
  |  Embed question → search query_memory → cache miss
  |
  |  Step 1: vector_search_tables (COMBINED QUERY)
  |  Embed question ONCE → search schema_embeddings + query_memory
  |  → "use kpi.markettrade" + cache few-shot examples
  |
  |  Step 2: load_yaml_metadata("markettrade", "nl2sql_omx_kpi")
  |  → 774 columns with descriptions, synonyms ("edge" = edge_bps)
  |
  |  Step 3: fetch_few_shot_examples (CACHE HIT — instant, no BQ call)
  |  → "here's how similar PnL questions were answered before"
  |
  |  Step 4: LLM generates SQL using table + columns + examples
  |  → SELECT ROUND(AVG(edge_bps), 4) FROM kpi.markettrade WHERE ...
  |
  |  Step 5: dry_run_sql → validates syntax, checks permissions
  |
  |  Step 6: execute_sql → runs query, returns results
  |
  v
ANSWER: "The average edge was 0.0234 bps across 1,247 market trades today."
```

---

## References

- [BigQuery: Introduction to embeddings and vector search](https://docs.cloud.google.com/bigquery/docs/vector-search-intro)
- [BigQuery: ML.GENERATE_EMBEDDING function](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-embedding)
- [BigQuery: Search embeddings with VECTOR_SEARCH](https://docs.cloud.google.com/bigquery/docs/vector-search)
- [Text Embeddings with OpenAI: A Practical Guide for 2026](https://thelinuxcode.com/text-embeddings-with-openai-a-practical-engineers-guide-for-2026/)
- [Cosine Similarity Is Not That Scary: An Intuitive Explanation](https://medium.com/@iyabivuzed/cosine-similarity-is-not-that-scary-an-intuitive-explanation-fcc819d93ad3)
- [Stack Overflow: An Intuitive Introduction to Text Embeddings](https://stackoverflow.blog/2023/11/09/an-intuitive-introduction-to-text-embeddings/)
