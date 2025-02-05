from airflow import DAG
from airflow.utils.task_group import TaskGroup

# Import all the functions and constants from your existing DAGs
from create_collections import *
from create_merged import *
from create_kv import *
from create_inlets import *
from update_es import *


with DAG(
    "collections-pipeline",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="End-to-end pipeline for creating and processing collections",
    start_date=CONFIG.start_date,
    schedule=CONFIG.run_interval,  # Or whatever schedule you want for the full pipeline
    catchup=False,
    tags=["collection-templates"],
) as dag:

    # Inlets Creation
    with TaskGroup('filter-wikidata') as wikidata_group:
        wikidata_group.doc_md = dedent("""
        Downloading and filtering Wikidata for entries including only specific subjects and predicates.
        """
        )

        regex = "'^(<https://en\.wikipedia\.org/wiki/|<http://www\.wikidata\.org/entity/).*((<http://www\.wikidata\.org/prop/direct/P18>|<http://www\.wikidata\.org/prop/direct/P1753>|<http://www\.wikidata\.org/prop/direct/P31>|<http://schema\.org/about>|<http://www\.wikidata\.org/prop/direct/P1754>|<http://www\.wikidata\.org/prop/direct/P4224>|<http://www\.wikidata\.org/prop/direct/P948>|<http://www\.wikidata\.org/prop/direct/P279>|<http://www\.wikidata\.org/prop/direct/P360>|<http://www\.w3\.org/2002/07/owl\#sameAs>)|((<http://schema\.org/description>|<http://schema\.org/name>|<http://www\.w3\.org/2000/01/rdf\-schema\#label>).*@en .$$))'"
        filter_wikidata_task = BashOperator(
            outlets=[WIKIDATA_FILTERED],
            task_id="grep-wikidata",
            cwd=f"{CONFIG.local_prefix}",
            bash_command=f"wget {wget_for_wikidata('truthy')} -q -O - | lbzip2 -d --stdout | grep -E {regex} | lbzip2 -c > {WIKIDATA_FILTERED.local_name()}",
            start_date=CONFIG.start_date,
        )

        filter_wikidata_task.doc_md = dedent(
            """\
        #### Task Documentation
        This task downloads and filters the predicates in the Wikipedia dump to include 
        only a subset of predicates and and a subset of entities (only wikidata and English wikipedia).
        """
        )

        upload_filtered_data_task = PythonOperator(
            task_id="backup-filtered-wikidata",
            python_callable=upload_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
                "local_path": WIKIDATA_FILTERED.local_name(),
                "remote_path": WIKIDATA_FILTERED.s3_name(),
            },
        )

        upload_filtered_data_task.doc_md = dedent(
            """\
        #### Task Documentation

        Upload filtered Wikidata dump to S3
        """
        )

        filter_wikidata_task >> upload_filtered_data_task

    with TaskGroup('download-wikipedia') as wikipedia_group:
        wikipedia_group.doc_md = dedent("""
        Downloading of Wikipedia dump files
        """  
        )
        
        pagelinks_task = BashOperator(
            outlets=[WIKIPEDIA_PAGELINKS],
            task_id="download-pagelinks",
            bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_PAGELINKS)}",
        )

        pagelinks_task.doc_md = dedent(
            """\
        #### Task Documentation
        Download the latest **pagelinks** file and reaname it for the current date.
        The current date requirement is set, because this is the requirement of the file parser.
        """
        )


        redirect_task = BashOperator(
            outlets=[WIKIPEDIA_REDIRECT],
            task_id="download-redirect",
            bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_REDIRECT)}",
        )

        redirect_task.doc_md = dedent(
            """\
        #### Task Documentation
        Download the latest **redirects** file.
        """
        )

        categorylinks_task = BashOperator(
            outlets=[WIKIPEDIA_CATEGORYLINKS],
            task_id="download-categorylinks",
            bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_CATEGORYLINKS)}",
        )

        categorylinks_task.doc_md = dedent(
            """\
        #### Task Documentation
        Download the latest **category links** file and reaname it for the current date.
        """
        )

        pageprops_task = BashOperator(
            outlets=[WIKIPEDIA_PAGEPROPS],
            task_id="download-pageprops",
            bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_PAGEPROPS)}",
        )

        # TODO make copy of page props for mapper
        pageprops_task.doc_md = dedent(
            """\
        #### Task Documentation
        Download the latest **page props** file and reaname it for the current date.
        """
        )

        page_task = BashOperator(
            outlets=[WIKIPEDIA_PAGE],
            task_id="download-page",
            bash_command=f"wget {wget_for_wikipedia(WIKIPEDIA_PAGE)}",
        )

        page_task.doc_md = dedent(
            """\
        #### Task Documentation
        Download the latest **page** file.
        """
        )

    with TaskGroup('wikimapper') as wikimapper_group:
        wikimapper_group.doc_md = dedent("""
        Loading of Wikipedia dump files to wikimapper database
        """  
        )

        wikimapper_task = BashOperator(
            outlets=[WIKIMAPPER],
            task_id="load-redirect",
            bash_command=f"wikimapper create enwiki-latest --dumpdir {CONFIG.local_prefix} --target {WIKIMAPPER.name()}",
        )

        wikimapper_task.doc_md = dedent("""
        #### Task Documentation
        Load the redirects to the wikimapper database.
        """
        )

        upload_wikimapper_task = PythonOperator(
            task_id="backup-wikimapper",
            python_callable=upload_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
                "local_path": WIKIMAPPER.local_name(),
                "remote_path": WIKIMAPPER.s3_name(),
            },
        )

        upload_wikimapper_task.doc_md = dedent("""
        #### Task Documentation

        Upload wikimapper data to S3.
        """
        )

        wikimapper_task >> upload_wikimapper_task
    
    with TaskGroup('qrank') as qrank_group:
        qrank_task = BashOperator(
            outlets=[QRANK],
            task_id="download-qrank",
            bash_command=f"wget -O - https://qrank.wmcloud.org/download/qrank.csv.gz | gunzip -c > {QRANK.local_name()}",
        )

        qrank_task.doc_md = dedent(
            """\
        #### Task Documentation

        Download Qrank file (file with ranks of Wikipedia pages) from wmcloud.
        """
        )
    
    with TaskGroup('namehash-files') as namehash_group:
        domains_task = PythonOperator(
            outlets=[SUGGESTABLE_DOMAINS],
            task_id="download-suggestable-domains",
            python_callable=download_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_download,
                "remote_path": SUGGESTABLE_DOMAINS.name(),
                "local_path": SUGGESTABLE_DOMAINS.local_name()
            },
        )
        domains_task.doc_md = dedent("""
        #### Task Documentation

        Download suggestable domains from namehash bucket.
        """
        )
        
        avatar_task = PythonOperator(
            outlets=[AVATAR_EMOJI],
            task_id="download-avatars-emojis",
            python_callable=download_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_download,
                "remote_path": AVATAR_EMOJI.name(),
                "local_path": AVATAR_EMOJI.local_name()
            },
        )
        avatar_task.doc_md = dedent("""
        #### Task Documentation

        Download Avatar file from namehash bucket.
        """
        )
    
    with TaskGroup('setup-environment') as setup_group:
        remove_files_task = PythonOperator(
            task_id="remove-files",
            python_callable=remove_intermediate_files
        )

        remove_files_task.doc_md = dedent("""
        #### Task Documentation

        Remove all temporary files in the airflow data directory.
        """
        )

        create_date_file_task = BashOperator(
            outlets=[PROCESS_START_DATE],
            task_id="create-date-file",
            bash_command=f"echo '{CONFIG.date_str}' > {PROCESS_START_DATE.local_name()}"
        )
        create_date_file_task.doc_md = dedent("""
        #### Task Documentation

        Create file with the date the new process for processing Wikipedia/Wikidata process started.
        It is used as the location for the buckup files in S3.
        """
        )

        create_buckup_directory_task = PythonOperator(
            task_id="create-buckup-directory",
            python_callable=create_buckup_directory,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
            },
        )

        create_buckup_directory_task.doc_md = dedent("""
        #### Task Documentation

        Create the buckup directory at S3.
        """
        )

        clear_dags_task = PythonOperator(
            task_id="clear-dags",
            python_callable=clear_dags,
            op_kwargs={
            },
        )

        clear_dags_task.doc_md = dedent("""
        #### Task Documentation

        Clear status of all DAGs related to collection templates.
        """
        )

        remove_files_task >> create_date_file_task >> create_buckup_directory_task >> clear_dags_task

    # KV creation
    with TaskGroup('rocksdb-main') as rocksdb_main_group:
        create_rocksdb_task = PythonOperator(
            task_id='create-rocksdb',
            python_callable=create_rocksdb,
            op_kwargs={
                "dbs": dbs, 
                "entity_path": WIKIDATA_FILTERED.local_name(),  
                "db_path_prefix": CONFIG.local_prefix
            },
            outlets=[ROCKS_DB_2, ROCKS_DB_3, ROCKS_DB_4, ROCKS_DB_5, ROCKS_DB_6,]
        )
        create_rocksdb_task.doc_md = dedent("""
        #### Task Documentation
        The task creates a number of rocksdb databases, to store mappings between
        wikidata entitites and their properties.

        These databases do not include the mapping between Wikidata and English Wikipedia.
        """
        )
    
    with TaskGroup('rocksdb-entities') as rocksdb_entities_group:
        create_reverse_task = PythonOperator(
            task_id='create-rocksdb-entities',
            python_callable=load_wikidata_wikipedia_mapping,
            op_kwargs={
                "input_path": WIKIMAPPER.local_name(), 
                "db1_path": ROCKS_DB_1.local_name(),  
                "db1_rev_path": ROCKS_DB_1_REVERSE.local_name(),  
            },
            outlets=[ROCKS_DB_1, ROCKS_DB_1_REVERSE]
        )
        create_reverse_task.doc_md = dedent("""
        #### Task Documentation
        The task creates a mapping from Wikidata to Wikipedia and from Wikipedia to Wikidata.
        """
        )

    # Collections creation
    with TaskGroup('categories-allowed') as categories_allowed_group:
        create_categories_task = PythonOperator(
            task_id='create-categories',
            python_callable=extract_collections,
            op_kwargs={
                "id_title_path": ROCKS_DB_1_REVERSE.local_name(),
                "members_type_path": ROCKS_DB_3.local_name(),
                "mode": 'category',
                "output": CATEGORIES.local_name()
            },
            outlets=[CATEGORIES]
        )
        create_categories_task.doc_md = dedent("""
        #### Task Documentation
        Create categories JSON file.

        This file contains the Wikidata items that are categories in the English Wikipedia.
        """
        )

        create_allowed_categories_task = PythonOperator(
            task_id='create-allowed-categories',
            python_callable=extract_titles,
            op_kwargs={
                "input": CATEGORIES.local_name(),
                "output": ALLOWED_CATEGORIES.local_name(),
            },
            outlets=[ALLOWED_CATEGORIES]
        )
        create_allowed_categories_task.doc_md = dedent("""
        #### Task Documentation
        Create allowed categories TXT file.

        This file contains the titles of the categories from the English Wikipedia that are registered in Wikidata.
        """
        )

        create_categories_task >> create_allowed_categories_task

    with TaskGroup('categories-enwiki') as categories_enwiki_group:
        create_categories_tsv_task = BashOperator(
            task_id="create-enwiki-categories-tsv",
            bash_command=f"unpigz -c {WIKIPEDIA_CATEGORYLINKS.latest_local_name()} | sql_parser 0 1 | pigz > {WIKIPEDIA_CATEGORYLINKS_TSV.local_name()}",
        )

        create_categories_tsv_task.doc_md = dedent("""
        #### Task Documentation
        Convert SQL dump file to TSV file with selected columns.

        The file contains associations between English Wikipedia categories and the pages that belong to those categories.
        """
        )

        create_categories_task = PythonOperator(
            task_id='create-category-links',
            python_callable=extract_associations_from_tsv,
            op_kwargs={
                "input": WIKIPEDIA_CATEGORYLINKS_TSV.local_name(), 
                "mode": 'category', 
                "output": CATEGORY_PAGES.local_name(),
                "allowed_values": ALLOWED_CATEGORIES.local_name(),
            },
            outlets=[CATEGORY_PAGES],
        )
        create_categories_task.doc_md = dedent("""
        #### Task Documentation
        Create file with category content and filter by allowed categories.

        The file contains associations between English Wikipedia categories and the pages that belong to those categories.
        """
        )

        create_categories_tsv_task >> create_categories_task

    with TaskGroup('lists-allowed') as lists_allowed_group:
        create_lists_task = PythonOperator(
            task_id='create-lists',
            python_callable=extract_collections,
            op_kwargs={
                "id_title_path": ROCKS_DB_1_REVERSE.local_name(),
                "members_type_path": ROCKS_DB_3.local_name(),
                "mode": 'list',
                "output": LISTS.local_name(),
            },
            outlets=[LISTS],
        )
        create_lists_task.doc_md = dedent("""
        #### Task Documentation
        Create list JSON file.

        This file contains the Wikidata items that are lists in the English Wikipedia.
        """
        )

        create_allowed_lists_task = PythonOperator(
            task_id='create-allowed-lists',
            python_callable=extract_page_ids,
            op_kwargs={
                "input": LISTS.local_name(),
                "output": ALLOWED_LISTS.local_name(),
                "wikimapper_path": WIKIMAPPER.local_name()
            },
            outlets=[ALLOWED_LISTS]
        )
        create_allowed_lists_task.doc_md = dedent("""
        #### Task Documentation
        Create allowed lists TXT file.

        This file contains the titles of the lists from the English Wikipedia that are registered in Wikidata.
        """
        )

        create_lists_task >> create_allowed_lists_task

    with TaskGroup('lists-enwiki') as lists_enwiki_group:
        create_lists_tsv_task = BashOperator(
            task_id="create-enwiki-lists-tsv",
            bash_command=f"unpigz -c {WIKIPEDIA_PAGELINKS.latest_local_name()} | sql_parser 0 2 | pigz > {WIKIPEDIA_PAGELINKS_TSV.local_name()}",
        )

        create_lists_tsv_task.doc_md = dedent("""
        #### Task Documentation
        Convert SQL dump file to TSV file with selected columns.

        The file contains associations between English Wikipedia lists and the pages that belong to those lists.
        """
        )

        create_enwiki_lists_task = PythonOperator(
            task_id='create-list-links',
            python_callable=extract_associations_from_tsv,
            op_kwargs={
                "input": WIKIPEDIA_PAGELINKS_TSV.local_name(), 
                "mode": 'list', 
                "output": LIST_PAGES.local_name(),
                "allowed_values": ALLOWED_LISTS.local_name(),
            },
            outlets=[LIST_PAGES]
        )
        create_enwiki_lists_task.doc_md = dedent("""
        #### Task Documentation
        Create file with list pages content.

        The file contains associations between English Wikipedia list pages and the pages that belong to those lists.
        """
        )

        create_lists_tsv_task >> create_enwiki_lists_task

    with TaskGroup('categories-mapped') as categories_mapped_group:
        create_mapped_categories_task = PythonOperator(
            task_id='create-mapped-categories',
            python_callable=map_to_titles,
            op_kwargs={
                "input": CATEGORY_PAGES.local_name(),
                "mode": 'category',
                "output": MAPPED_CATEGORIES.local_name(),
                "wikimapper_path": WIKIMAPPER.local_name(),
            },
        )
        create_mapped_categories_task.doc_md = dedent("""
        #### Task Documentation
        Create file with the mapped categories.

        The resulting file contains a mapping from a collection to the member with both the collection and the member referenced by title.
        """
        )

        create_sorted_categories_task = BashOperator(
            outlets=[SORTED_CATEGORIES],
            task_id="sort-categories",
            bash_command=f"(head -n 1 {MAPPED_CATEGORIES.local_name()} && tail -n +2 {MAPPED_CATEGORIES.local_name()} | " +
                f"LC_ALL=C sort) > {SORTED_CATEGORIES.local_name()}",
        )

        create_sorted_categories_task.doc_md = dedent("""
        #### Task Documentation
        Create file with the sorted categories.

        The resulting file contains a mapping from a collection to the member sorted by the collection name.
        """
        )

        create_mapped_categories_task >> create_sorted_categories_task

    with TaskGroup('lists-mapped') as lists_mapped_group:
        create_mapped_lists_task = PythonOperator(
            task_id='create-mapped-lists',
            python_callable=map_to_titles,
            op_kwargs={
                "input": LIST_PAGES.local_name(),
                "mode": 'list',
                "output": MAPPED_LISTS.local_name(),
                "wikimapper_path": WIKIMAPPER.local_name(),
            },
        )
        create_mapped_lists_task.doc_md = dedent("""
        #### Task Documentation
        Create file with the mapped lists.

        The resulting file contains a mapping from a collection to the member with both the collection and the member referenced by title.
        """
        )

        create_sorted_lists_task = BashOperator(
            outlets=[SORTED_LISTS],
            task_id="sort-lists",
            bash_command=f"(head -n 1 {MAPPED_LISTS.local_name()} && tail -n +2 {MAPPED_LISTS.local_name()} | " +
                f"LC_ALL=C sort) > {SORTED_LISTS.local_name()}",
        )

        create_sorted_lists_task.doc_md = dedent("""
        #### Task Documentation
        Create file with the sorted lists.

        The resulting file contains a mapping from a collection to the member sorted by the collection name.
        """
        )

        create_mapped_lists_task >> create_sorted_lists_task

    with TaskGroup('categories-members') as categories_members_group:
        create_category_members_task = PythonOperator(
            task_id='create-category-members',
            python_callable=reformat_csv_to_json,
            op_kwargs={
                "input": SORTED_CATEGORIES.local_name(),
                "output": CATEGORY_MEMBERS.local_name(),
                "collections_json": CATEGORIES.local_name(),
            },
        )
        create_category_members_task.doc_md = dedent("""
        #### Task Documentation
        Change CSV **categories** file, to JSON format.

        The task combines the Wikipedia names with Wikidata structured info resulting in a JSON object containing the info about the
        collection and its members.
        """
        )

        validate_category_members_task = PythonOperator(
            task_id='validate-category-members',
            python_callable=validate_members,
            op_kwargs={
                "input": CATEGORY_MEMBERS.local_name(),
                "output": VALIDATED_CATEGORY_MEMBERS.local_name(),
                "title_id_path": ROCKS_DB_1.local_name(),
                "id_type_path": ROCKS_DB_2.local_name(),
                "same_as_path": ROCKS_DB_6.local_name(),
                "wikimapper_path": WIKIMAPPER.local_name(),
            },
            outlets=[VALIDATED_CATEGORY_MEMBERS],
        )

        validate_category_members_task.doc_md = dedent("""
        #### Task Documentation
        Validate correctness of **category** members.

        The methods checks if the members of a collection have type compatible with the collection's type.
        """
        )

        upload_category_members_task = PythonOperator(
            task_id="backup-category-members",
            python_callable=upload_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
                "local_path": VALIDATED_CATEGORY_MEMBERS.local_name(),
                "remote_path": VALIDATED_CATEGORY_MEMBERS.s3_name(),
            },
        )

        upload_category_members_task.doc_md = dedent("""
        #### Task Documentation

        Upload category members data to S3.
        """
        )

        create_category_members_task >> validate_category_members_task >> upload_category_members_task
    
    with TaskGroup('lists-members') as lists_members_group:
        create_list_members_task = PythonOperator(
            task_id='create-list-members',
            python_callable=reformat_csv_to_json,
            op_kwargs={
                "input": SORTED_LISTS.local_name(),
                "output": LIST_MEMBERS.local_name(),
                "collections_json": LISTS.local_name(),
            },
        )
        create_list_members_task.doc_md = dedent("""
        #### Task Documentation
        Change CSV **lists** file, to JSON format.

        The task combines the Wikipedia names with Wikidata structured info resulting in a JSON object containing the info about the
        collection and its members.
        """
        )

        validate_list_members_task = PythonOperator(
            task_id='validate-list-members',
            python_callable=validate_members,
            op_kwargs={
                "input": LIST_MEMBERS.local_name(),
                "output": VALIDATED_LIST_MEMBERS.local_name(),
                "title_id_path": ROCKS_DB_1.local_name(),
                "id_type_path": ROCKS_DB_2.local_name(),
                "same_as_path": ROCKS_DB_6.local_name(),
                "wikimapper_path": WIKIMAPPER.local_name(),
            },
            outlets=[VALIDATED_LIST_MEMBERS],
        )

        validate_list_members_task.doc_md = dedent("""
        #### Task Documentation
        Validate correctness of **list** members.

        The methods checks if the members of a collection have type compatible with the collection's type.
        """
        )

        upload_list_members_task = PythonOperator(
            task_id="backup-list-members",
            python_callable=upload_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
                "local_path": VALIDATED_LIST_MEMBERS.local_name(),
                "remote_path": VALIDATED_LIST_MEMBERS.s3_name(),
            },
        )

        upload_list_members_task.doc_md = dedent("""
        #### Task Documentation

        Upload list members data to S3.
        """
        )

        create_list_members_task >> validate_list_members_task >> upload_list_members_task

    # Merging collections
    with TaskGroup('interesting-score-cache') as interesting_score_cache_group:
        create_list_members_task = PythonOperator(
            task_id='create-unique-list-members',
            python_callable=compute_unique_members,
            op_kwargs={
                "input": VALIDATED_LIST_MEMBERS.local_name(), 
                "force_normalize_path": FORCE_NORMALIZE_CACHE.local_name(), 
                "output": UNIQ_LIST_MEMBERS.local_name(), 
            },
        )
        create_list_members_task.doc_md = dedent("""
        #### Task Documentation
        Create file with unique list members.
        """
        )

        create_category_members_task = PythonOperator(
            task_id='create-unique-category-members',
            python_callable=compute_unique_members,
            op_kwargs={
                "input": VALIDATED_CATEGORY_MEMBERS.local_name(), 
                "force_normalize_path": FORCE_NORMALIZE_CACHE.local_name(), 
                "output": UNIQ_CATEGORY_MEMBERS.local_name(), 
            },
        )
        create_category_members_task.doc_md = dedent("""
        #### Task Documentation
        Create file with unique category members.
        """
        )

        create_cache_task = PythonOperator(
            task_id='create-cache',
            python_callable=cache_interesting_score,
            op_kwargs={
                "list_members": UNIQ_LIST_MEMBERS.local_name(), 
                "category_members": UNIQ_CATEGORY_MEMBERS.local_name(), 
                "interesting_score_path": INTERESTING_SCORE_CACHE.local_name(), 
            },
            outlets=[INTERESTING_SCORE_CACHE]
        )
        create_cache_task.doc_md = dedent("""
        #### Task Documentation
        Create cache for normalized names.
        """
        )

        create_list_members_task >> create_category_members_task >> create_cache_task
    
    with TaskGroup('collections-all-info') as collections_all_info_group:
        create_list_members_final_task = PythonOperator(
            task_id='create-list-members-final',
            python_callable=compute_all_info,
            op_kwargs={
                "input": VALIDATED_LIST_MEMBERS.local_name(), 
                "output": LIST_MEMBERS_ALL_INFO.local_name(), 
                "interesting_score_path": INTERESTING_SCORE_CACHE.local_name(), 
                "force_normalize_path": FORCE_NORMALIZE_CACHE.local_name(), 
                "qrank_path": QRANK.local_name(), 
                "domains_path": SUGGESTABLE_DOMAINS.local_name(), 
                "auxiliary_data_path": ROCKS_DB_5.local_name(), 
                "wikimapper_path": WIKIMAPPER.local_name(), 
            },
            outlets=[LIST_MEMBERS_ALL_INFO],
        )
        create_list_members_final_task.doc_md = dedent("""
        #### Task Documentation
        Create file with list members supplemented with the final information.
        """
        )

        create_category_members_final_task = PythonOperator(
            task_id='create-category-members-final',
            python_callable=compute_all_info,
            op_kwargs={
                "input": VALIDATED_CATEGORY_MEMBERS.local_name(), 
                "output": CATEGORY_MEMBERS_ALL_INFO.local_name(), 
                "interesting_score_path": INTERESTING_SCORE_CACHE.local_name(), 
                "force_normalize_path": FORCE_NORMALIZE_CACHE.local_name(), 
                "qrank_path": QRANK.local_name(), 
                "domains_path": SUGGESTABLE_DOMAINS.local_name(), 
                "auxiliary_data_path": ROCKS_DB_5.local_name(), 
                "wikimapper_path": WIKIMAPPER.local_name(), 

            },
            outlets=[CATEGORY_MEMBERS_ALL_INFO],
        )
        create_category_members_final_task.doc_md = dedent("""
        #### Task Documentation
        Create file with list members supplemented with the final information.
        """
        )

        create_list_members_final_task >> create_category_members_final_task

    with TaskGroup('collections-merge') as collections_merge_group:
        merge_lists_and_categories_task = PythonOperator(
            task_id='merge-lists-categories',
            python_callable=merge_lists_and_categories,
            op_kwargs={
                "list_members": LIST_MEMBERS_ALL_INFO.local_name(), 
                "category_members": CATEGORY_MEMBERS_ALL_INFO.local_name(), 
                "output": MERGED_COLLECTIONS.local_name(), 
                "related_data_path": ROCKS_DB_4.local_name(), 
            },
        )
        merge_lists_and_categories_task.doc_md = dedent("""
        #### Task Documentation
        Merge list and category members.
        """
        )


        upload_merged_task = PythonOperator(
            task_id="backup-merged-collection-members",
            python_callable=upload_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
                "local_path": MERGED_COLLECTIONS.local_name(),
                "remote_path": MERGED_COLLECTIONS.s3_name(),
            },
        )

        upload_merged_task.doc_md = dedent("""
        #### Task Documentation

        Upload merged collection members data to S3.
        """
        )

        remove_letters_task = PythonOperator(
            task_id='remove-letters',
            python_callable=remove_collections_with_letters,
            op_kwargs={
                "input": MERGED_COLLECTIONS.local_name(), 
                "output": WITHOUT_LETTERS.local_name(), 
            },
        )
        remove_letters_task.doc_md = dedent("""
        #### Task Documentation
        Remove collections ending with letters.
        """
        )

        remove_duplicates_task = PythonOperator(
            task_id='remove-duplicates',
            python_callable=remove_duplicates,
            op_kwargs={
                "input": WITHOUT_LETTERS.local_name(), 
                "output": WITHOUT_DUPLICATES.local_name(), 
            },
        )
        remove_duplicates_task.doc_md = dedent("""
        #### Task Documentation
        Remove duplicates in collections.
        """
        )

        final_processing_task = PythonOperator(
            task_id='final-processing',
            python_callable=collection_factory,
            outlets=[MERGED_FINAL],
            op_kwargs={
                "input": WITHOUT_DUPLICATES.local_name(), 
                "output": MERGED_FINAL.local_name(), 
                "name_to_hash_path": NAME_TO_HASH_CACHE.local_name(), 
                "avatar_path": AVATAR_EMOJI.local_name(), 
            },
        )
        final_processing_task.doc_md = dedent("""
        #### Task Documentation
        Create final representation of collections.
        """
        )
        
        upload_final_task = PythonOperator(
            task_id="backup-final-merged",
            python_callable=upload_s3_file,
            op_kwargs={
                "bucket": CONFIG.s3_bucket_upload,
                "local_path": MERGED_FINAL.local_name(),
                "remote_path": MERGED_FINAL.s3_name(),
            },
        )

        upload_final_task.doc_md = dedent("""
        #### Task Documentation

        Upload final collection members data to S3.
        """
        )

        merge_lists_and_categories_task >> upload_merged_task >> remove_letters_task >> remove_duplicates_task >> final_processing_task >> upload_final_task

    # Updating Elasticsearch
    with TaskGroup('update-es') as update_es_group:
        produce_update_operations_task = PythonOperator(
            outlets=[UPDATE_OPERATIONS],
            task_id='produce-update-operations',
            python_callable=produce_update_operations,
            op_kwargs={
                "previous": PREVIOUS_MERGED_FINAL.local_name(),
                "current": MERGED_FINAL.local_name(),
                "output": UPDATE_OPERATIONS.local_name(),
            },
        )
        produce_update_operations_task.doc_md = dedent("""
        #### Task Documentation
        Produce update operations.
        """
        )

        apply_operations_task = PythonOperator(
            task_id='apply-operations',
            python_callable=apply_operations,
            op_kwargs={
                "operations": UPDATE_OPERATIONS.local_name(),
            },
        )
        apply_operations_task.doc_md = dedent("""
        #### Task Documentation
        Apply update operations.
        """
        )

        archive_merged_final_task = PythonOperator(
            task_id="archive-merged-final",
            python_callable=archive_merged_final,
            outlets=[PREVIOUS_MERGED_FINAL],
            op_kwargs={
                "original": MERGED_FINAL.local_name(),
                "latest": PREVIOUS_MERGED_FINAL.local_name(),
                "archived": MERGED_FINAL.local_name(prefix='archived_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S_'))
            },
        )
        archive_merged_final_task.doc_md = dedent("""
        #### Task Documentation
        Archive current merged final, and update a symlink to it, so that it can be used
        as a reference for computing differences in the next run.
        """
        )

        produce_update_operations_task >> apply_operations_task >> archive_merged_final_task

    # Definine group-level DAG

    # Inlets
    setup_group >> [
        wikipedia_group,
        wikidata_group,
        qrank_group,
        namehash_group,
    ]

    # Processing Wikipedia and Wikidata
    wikidata_group >> rocksdb_main_group
    wikipedia_group >> wikimapper_group
    wikimapper_group >> rocksdb_entities_group

    # After processing Wikipedia and Wikidata we start forming collections
    rocksdb_main_group >> [
        categories_allowed_group,
        lists_allowed_group,
    ]

    rocksdb_entities_group >> [
        categories_allowed_group,
        lists_allowed_group,
    ]

    categories_allowed_group >> categories_enwiki_group >> categories_mapped_group >> categories_members_group
    lists_allowed_group >> lists_enwiki_group >> lists_mapped_group >> lists_members_group

    # After forming collections we start computing interesting score
    categories_members_group >> interesting_score_cache_group
    lists_members_group >> interesting_score_cache_group

    # After having formed collections and computed interesting score, we start computing final information
    # This depends on the QRank and NameHash files
    interesting_score_cache_group >> collections_all_info_group
    qrank_group >> collections_all_info_group
    namehash_group >> collections_all_info_group

    # After having computed final information, we start merging collections
    collections_all_info_group >> collections_merge_group >> update_es_group
