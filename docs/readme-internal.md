# Collections Templates Pipeline

A sophisticated Apache Airflow pipeline for creating and managing topic-based name collections enriched with metadata and semantic relationships.

## Overview

This project leverages the vast knowledge graph of Wikipedia and Wikidata to create a comprehensive network of meaningful name collections for the ENS ecosystem. By analyzing semantic relationships, categories, and metadata from these knowledge bases, we build thematic collections of names that are naturally related and contextually relevant.

The system enables discovery of names through their conceptual connections - whether they belong to the same category, share similar attributes, or are commonly associated together. This creates an intuitive way for users to explore and find meaningful names based on their interests and use cases.

## 🌐 Wikipedia Pipeline DAGs

![Pipeline DAG](media/makefile-dag.png)

### [📥](airflow/dags/create_inlets.py) Filter Wikidata DAG
   - Downloads and filters Wikidata dump to include only specific predicates and English Wikipedia entities
   - Uploads filtered data to S3 for backup

### [📥](airflow/dags/create_inlets.py) Download Wikipedia DAG 
   - Downloads latest Wikipedia dump files including pagelinks, redirects, category links, page props and page data
   - Runs with limited concurrency to manage resource usage

### [📥](airflow/dags/create_inlets.py) Wikimapper DAG
   - Loads Wikipedia redirect data into Wikimapper database for entity mapping
   - Uploads Wikimapper database to S3 for backup

### [📥](airflow/dags/create_inlets.py) QRank DAG
   - Downloads QRank file containing Wikipedia page ranks
   - Used for scoring and ranking collections

### [📥](airflow/dags/create_inlets.py) Namehash Files DAG
   - Downloads suggestable domains list from S3
   - Downloads avatar/emoji mappings for collection avatars

### [📥](airflow/dags/create_inlets.py) Setup Environment DAG
   - Cleans up temporary files
   - Creates date file for new processing run
   - Sets up S3 backup directory
   - Clears status of collection template DAGs

---

### [🛢](airflow/dags/create_kv.py) RocksDB Main DAG
   - Creates multiple RocksDB key-value stores from filtered Wikidata:
     - DB2: Maps Wikidata IDs to their instance types and subclass relationships
     - DB3: Maps Wikidata IDs to list/category membership predicates ('is_a_list_of', 'category_contains')
     - DB4: Maps Wikidata IDs to list-category relationship predicates
     - DB5: Maps Wikidata IDs to auxiliary data (names, labels, descriptions, images, banners)
     - DB6: Maps Wikidata IDs to their sameAs relationships
   - Uploads databases to S3 for backup

### [🛢](airflow/dags/create_kv.py) RocksDB Entities DAG
   - Creates bidirectional mappings between Wikipedia and Wikidata:
     - DB1: Maps Wikipedia article titles to their Wikidata IDs
     - DB1 Reverse: Maps Wikidata IDs to their Wikipedia article titles
   - Uses Wikimapper database as source
   - Uploads databases to S3 for backup

---

### [🗃️](airflow/dags/create_collections.py) Categories Allowed DAG
   - Creates JSON file of Wikidata items that are English Wikipedia categories
   - Extracts category titles for filtering

### [🗃️](airflow/dags/create_collections.py) Categories EnWiki DAG
   - Processes Wikipedia category links dump into TSV format
   - Filters category associations using allowed categories list

### [🗃️](airflow/dags/create_collections.py) Categories Mapped DAG
   - Maps category and member IDs to their Wikipedia titles
   - Sorts mapped categories for efficient processing

### [🗃️](airflow/dags/create_collections.py) Categories Members DAG
   - Reformats category data into JSON with structured collection info
   - Validates member types against collection types
   - Uploads validated data to S3

### [🗃️](airflow/dags/create_collections.py) Lists Allowed DAG
   - Creates JSON file of Wikidata items that are English Wikipedia lists
   - Extracts list page IDs for filtering

### [🗃️](airflow/dags/create_collections.py) Lists EnWiki DAG
   - Processes Wikipedia pagelinks dump into TSV format
   - Filters list associations using allowed lists

### [🗃️](airflow/dags/create_collections.py) Lists Mapped DAG
   - Maps list and member IDs to their Wikipedia titles
   - Sorts mapped lists for efficient processing

### [🗃️](airflow/dags/create_collections.py) Lists Members DAG
   - Reformats list data into JSON with structured collection info
   - Validates member types against collection types
   - Uploads validated data to S3

---

### [🔀](airflow/dags/create_merged.py) Interesting Score Cache DAG
   - Creates caches for normalized names and uses [NameAI](https://nameai.io/) to calculate interesting scores
   - Processes unique list and category members
   - Uploads cache to S3 for reuse

### [🔀](airflow/dags/create_merged.py) Collections Merge DAG
   - Merges list and category members into unified collections
   - Processes collections through multiple stages:
     - Removes collections with letter-based suffixes
     - Deduplicates and merges similar collections
     - Enriches with metadata (ranks, scores, statuses)
     - Generates collection avatars and banners
   - Uploads final merged collections to S3

---

### [📤](airflow/dags/update_es.py) Update Elasticsearch DAG
   - Computes differences between current and previous collection states
   - Generates optimized update operations:
     - Creates new collection documents
     - Updates changed fields in existing collections
     - Marks removed collections as archived
   - Handles large collections (>10k members) gracefully
   - Resolves ID conflicts during document creation
   - Archives current collection state for future comparisons
   - Uploads archived collections to S3


## 🖇 Related Collections Pipeline DAG

The precompute-related-collections DAG generates and updates related collection recommendations by:

1. Setting Up NameGraph Service
   - Clones latest NameGraph repo from prod branch (`clone_name_generator_task`)
   - Launches service with required configs via Docker (`launch_name_generator_task`) 
   - Waits for service to be ready (`wait_for_name_generator_task`)

2. Generating Related Collections (`generate_related_collections_task`)
   - Queries all collections from Elasticsearch
   - For each collection, requests related recommendations from NameGraph:
     - Limits to 10 related collections per source
     - Enforces type diversity (max 2 per type) 
     - Uses name diversity ratio of 0.5
     - Includes collection IDs, titles and types
   - Stores results in JSONL format with update operations

3. Updating Elasticsearch (`apply_updates_task`)
   - Bulk updates ES documents with related collection data
   - Updates name_generator.related_collections field
   - Updates name_generator.related_collections_count
   - Handles failures gracefully with warnings

4. Cleanup
   - Stops NameGraph service after processing (`stop_name_generator_task`)

The pipeline ensures collections have diverse, relevant recommendations while maintaining type variety. It runs on-demand to refresh recommendations as collections evolve.

**Note:** The "NameGenerator" is a deprecated name. The service is now called NameGraph.

## ✨ Custom Collections Pipeline DAG

The custom collections pipeline processes user-created name collections, enriching them with metadata and loading them into Elasticsearch. The pipeline consists of the following steps:

1. Download Input Data (Parallel)
   - Download custom collections JSONL from S3 (`download_custom_collections_task`)
   - Download suggestable domains CSV from S3 for domain status validation (`download_suggestable_domains_task`)
   - Download avatar/emoji mappings for collection avatars (`download_avatars_emojis_task`)

2. Prepare Collections (`prepare_custom_collections_task`)
   - Normalize and tokenize member names
   - Calculate collection and member metrics:
     - Interesting scores
     - Name ranks
     - Domain status statistics
   - Generate collection metadata:
     - Banner images
     - Avatar emojis
     - Creation timestamps
     - Member counts and ratios

3. Generate ES Operations (`produce_update_operations_task`)
   - Map collection IDs to existing ES documents
   - Create update operations for existing collections
   - Generate create operations for new collections

4. Update Elasticsearch (`update_elasticsearch_task`)
   - Execute the generated operations to update the ES index
   - Create new collection documents
   - Update existing collection metadata

The pipeline runs on a schedule and handles both new collection creation and updates to existing collections. It enriches the raw collection data with computed metrics, validates domain statuses, and maintains consistency in the Elasticsearch index.
