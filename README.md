# Collocation Extraction using MapReduce  

## Problem Overview

The goal of this assignment is to extract statistically significant **word collocations** from a very large corpus (Google N-Grams) using a scalable MapReduce pipeline. 

For each **language (English / Hebrew)** and for each **decade**, we aim to output the **Top-100 strongest bigrams** according to the **Log-Likelihood Ratio (LLR)** measure.

For a bigram (w1, w2) in a given language and decade, LLR requires:

- c12: count of the bigram (w1, w2)
- c1: unigram count of w1
- c2: unigram count of w2
- N: total number of unigrams in that language and decade (after stopword filtering)

The solution is explicitly designed to operate under strict scalability constraints, without assuming that any large subset of the data can fit in memory.
---

## Key Design Principle: Streaming Joins with Secondary Sort

A central challenge in this task is performing joins **without storing large lists of bigrams or unigrams in memory**.

The core mechanism used throughout this solution is:

**Ensuring that UNIGRAM records arrive before all related BIGRAM records in the reducer, enabling streaming joins without buffering.**

This is achieved using:
- Explicit **secondary sort**, UNIGRAM records before BIGRAM/P records.
- Custom **partitioner**, same language, decade, and join word
- Custom **grouping comparator**, (language, decade, w1) or (language, decade, w2)
- Streaming reducers implemented with `run()` + `nextKeyValue()`

At no point does the system assume that all records for a word, decade, or language can fit in memory.

---

## Core Distributed Processing Concepts

- **secondary sort**  
  A technique that sorts records within the same reduce-group using a composite key, allowing enforcement of a required internal order (e.g., UNIGRAM records before BIGRAM/P records) without buffering data in memory.

- **partitioner**  
  Determines which reducer a key is sent to.  
  It ensures that all records required for a join group (e.g., same language, decade, and join word) are routed to the same reducer, while still distributing different groups across reducers for scalability.

- **grouping comparator**  
  Defines which keys belong to the same reduce-group.  
  It groups records by a prefix of the key (e.g., (language, decade, w1) or (language, decade, w2)), while allowing full-key sorting inside the group (language, decade, w1, sortType, w2).

- **Streaming reducers implemented with `run()` + `nextKeyValue()`**  
  A reducer pattern that processes records one-by-one while accessing the full composite key.  
  This enables true streaming joins and explicit group handling without collecting large value lists in memory.


- The **partitioner** guarantees that all records needed for a join arrive at the same reducer.
- The **grouping comparator** defines the logical join group.
- The **secondary sort** enforces the correct processing order inside the group.

Together, these mechanisms enable scalable, memory-safe joins that respect the assignment’s constraints.

---

## Job 1 – Join on w1 and Computation of N

### Goal

For each bigram (w1, w2), Job 1 attaches:
- c12 (bigram count)
- c1 (unigram count of w1)

In addition, Job 1 computes:
- N = total number of unigrams per (language, decade)

---

### Inputs

Job 1 uses `MultipleInputs` with `SequenceFileInputFormat`:

- English 1-grams  
- Hebrew 1-grams  
- English 2-grams  
- Hebrew 2-grams  

Each record contains:
- unigram or bigram, year and occurrence count (w, year, occurrence)/(w1 w2, year, occurrence).

Stopwords for English and Hebrew are loaded via the distributed cache.

---

### Mappers

#### Unigram Mapper
- Cleans tokens
- Filters stopwords
- Emits **UNI records** with key (language, decade, w1, typeOrder = 0, "") and value (UNI, c1)

#### Bigram Mapper
- Splits bigram into w1 and w2
- Cleans tokens
- Filters stopwords
- Emits **BIGRAM records** with key (language, decade, w1, typeOrder = 1, w2) and value (BIGRAM, c12)
grouped by w1 and carrying w2 in the key

---

### Secondary Sort Design 

**Key structure:**

(lang, decade, w1, typeOrder, w2)

Where:
- typeOrder = 0 → UNIGRAM  
- typeOrder = 1 → BIGRAM  

**Grouping comparator:**

(lang, decade, w1)

This guarantees that all records for the same w1 (within a language and decade) are processed together, and that the UNIGRAM record always arrives before all BIGRAM records.

This ordering is critical for streaming.

---

### Reducer (Streaming)

Job 1 uses a **true streaming reducer**, implemented using `run()` and `nextKeyValue()` instead of the standard `reduce()` method.

The standard `reduce(key, Iterable<values>)` API does not allow access to the full composite key (including w2), which is required for correct streaming joins.

Reducer logic:
1. Read UNIGRAM records and accumulate c1
2. For each subsequent BIGRAM record:
   - Emit a partial record containing (c12, c1)
3. Accumulate c1 values to compute N(lang, decade)

---

### Outputs (MultipleOutputs)

Job 1 produces two outputs:

1. **DATA output**

   Contains:
   - UNIGRAM records  
     UNI   lang   decade   w    c

   - Partial BIGRAM records  
     P     lang   decade   w2   w1   c12   c1

2. **N output (side output)**

   Contains:
   N   lang   decade   N

This output is relatively small and is consumed by Job 2 via the distributed cache.

---

## Job 2 – Join on w2 and LLR Computation

### Goal

For each partial record produced by Job 1, Job 2 attaches:
- c2 (unigram count of w2)
- N (loaded from Job 1 side output)

It then computes:
LLR(c1, c2, c12, N)

---

### Inputs

- DATA output from Job 1 
- N files from Job 1 loaded via distributed cache

---

### Secondary Sort Design 

**Key structure:**

(lang, decade, w2, sortType, w1)

Where:
- sortType = 0 → UNIGRAM  
- sortType = 1 → PARTIAL bigram  

**Grouping comparator:**

(lang, decade, w2)

This guarantees that for each w2, the unigram count c2 arrives before all partial bigram records that reference w2.

---

### Reducer (Streaming)

Job 2 again uses a **streaming reducer** with `run()` and `nextKeyValue()`.

Reducer logic:
1. Read UNIGRAM record and obtain c2
2. Load N(lang, decade) from the distributed cache
3. For each partial record:
   - Compute LLR
   - Emit (lang, decade) → (w1 w2, llr)

No combiner is used in Job 2, as correctness depends on seeing both unigram and partial records together.

---

## Job 3 – Top-100 Selection

### Goal

Select the **Top-100 bigrams per (language, decade)** according to their LLR scores.

---

### Mapper

Parses Job 2 output and emits:

key = (lang, decade)  
value = (bigram, llr)

---

### Combiner (Top-K)

The combiner maintains a **bounded min-heap of size 100** per key:
- Discards low-scoring bigrams early
- Emits at most 100 candidates per mapper

This dramatically reduces shuffle volume.

---

### Reducer

The reducer merges all local Top-100 heaps and retains only the global Top-100 collocations.

---

## Memory Safety and Scalability

The system makes **no assumptions** about the ability to store large datasets in memory.

All joins are performed using secondary sort and streaming reducers.

In-memory data structures are strictly bounded:
- Top-100 heaps (constant size)
- Stopword sets (small and fixed)

This guarantees scalability to very large corpora.

---

## Combiner Usage

- **Job 1 uses a combiner** because it emits a very large number of duplicate counting keys (unigrams and bigrams).  
  The combiner performs local aggregation on each mapper, summing counts before the shuffle phase, which significantly reduces network traffic.

- **Job 3 uses a combiner** to implement a local Top-100 selection.  
  Each mapper keeps only its top candidates, dramatically reducing the number of records sent to reducers.

- **Job 2 does not use a combiner** because correctness depends on seeing both unigram records and partial bigram records together. Applying a combiner could drop or reorder required records and break the join logic.

---

## End-to-End Flow Summary

- Job 1 joins bigrams with c1 and computes N
- Job 2 joins with c2 and computes LLR
- Job 3 selects the Top-100 collocations per language and decade

This pipeline correctly and efficiently implements large-scale statistical collocation extraction.

---

## How to Run (AWS EMR)

### Prerequisites
The way to run this project is using **Amazon EMR**, a managed Hadoop service provided by AWS.  
EMR eliminates the need to install or configure Hadoop manually.

### Create an EMR Cluster
We chose Hardware configuration:
   - **Master node**: `m4.large`
   - **Core nodes**: 2
Use default networking and the `EMR_DefaultRole` IAM role.

### Upload Files to S3
All inputs and outputs must be stored in Amazon S3.

Upload:
- Stopwords file
- The compiled JAR

structure:
s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data - Public Datasets
s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data - Public Datasets
s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data - Public Datasets
s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data - Public Datasets
s3://hadoop-counter/eng-stopwords.txt
s3://hadoop-counter/heb-stopwords.txt
s3://hadoop-counter/collocations.jar
s3://hadoop-counter-output

### Run the Job
1. Add a **Step** to the EMR cluster.
2. Select **Custom JAR**.
3. Set the JAR path.
