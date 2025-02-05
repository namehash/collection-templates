from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator

from elasticsearch import Elasticsearch, ConflictError
from elasticsearch.helpers import streaming_bulk, scan

from typing import Optional, Any
from datetime import datetime
import jsonlines
import hashlib
import random
import string
import shutil
import tqdm
import time
import json
import os

from create_inlets import CONFIG, CollectionDataset, ElasticsearchConfig
from create_merged import MERGED_FINAL

UPDATE_OPERATIONS = CollectionDataset(f"{CONFIG.remote_prefix}update-operations.jsonl")
PREVIOUS_MERGED_FINAL = CollectionDataset(
    f"{CONFIG.remote_prefix}archived_{CONFIG.elasticsearch.index}_latest_merged_final.jsonl"
)


COMPARING_FIELDS = ['data', 'template', 'metadata.members_count', 'metadata.collection_name_log_probability']
UPDATING_FIELDS = ['data', 'template', 'metadata.modified', 'metadata.members_count',
                   'metadata.collection_name_log_probability']


def connect_to_elasticsearch(config: ElasticsearchConfig):
    return Elasticsearch(
        hosts=[{
            'scheme': config.scheme,
            'host': config.host,
            'port': config.port
        }],
        http_auth=(config.username, config.password),
        timeout=60,
        http_compress=True,
    )


def collect_ids_mapping(es: Elasticsearch, index: str) -> dict[str, str]:
    query = {
        'query': {
            'match_all': {}
        },
        '_source': ['metadata.id'],
    }

    mapping = dict()
    for doc in tqdm.tqdm(
            scan(es, index=index, query=query),
            total=es.count(index=index)['count'],
            desc='collecting IDs mapping'
    ):
        mapping[doc['_source']['metadata']['id']] = doc['_id']

    return mapping


class JSONLIndex:
    def __init__(self, filepath: str, fields: list[str], ids_mapping: dict[str, str]):
        self.filepath = filepath
        self.fields = sorted(fields)
        self.ids_mapping = ids_mapping

        self.id2hash = dict()
        self.id2offset = dict()

    def build(self) -> None:
        with open(self.filepath, 'r', encoding='utf-8') as f:
            while True:
                start_pos = f.tell()
                line = f.readline()

                # EOF, empty lines before are returned as '\n'
                if not line:
                    break

                # skip empty lines
                if line == '\n':
                    continue

                # skip invalid JSON lines
                try:
                    data = json.loads(line)
                    obj_id = data['metadata']['id']
                except json.JSONDecodeError:
                    print('JSONDecodeError:', line)
                    continue

                self.id2hash[obj_id] = self.hash(data)
                self.id2offset[obj_id] = start_pos

    def hash(self, document: dict) -> str:
        hash = hashlib.sha256()
        for field in self.fields:
            # field may be a path to a nested field
            value = get_nested_field(document, field)
            hash.update(json.dumps(value, sort_keys=True).encode('utf-8'))
        return hash.hexdigest()

    def get_hash(self, id: str) -> str:
        return self.id2hash[id]

    def get_document(self, id: str) -> tuple[str, dict]:
        with open(self.filepath, 'r', encoding='utf-8') as f:
            f.seek(self.id2offset[id])
            document = json.loads(f.readline())
            return self.ids_mapping[document['metadata']['id']], document


def get_nested_field(document: dict, field: str, default: Any = None) -> Any:
    value = document
    for part in field.split('.'):
        value = value.get(part, None)
        if value is None:
            return default
    return value


def set_nested_field(document: dict, field: str, value: Any) -> None:
    placeholder = document
    splitted_field = field.split('.')
    for part in splitted_field[:-1]:
        placeholder = placeholder.setdefault(part, {})
    placeholder[splitted_field[-1]] = value


def generate_id(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits + '_'
    return ''.join(random.choice(alphabet) for _ in range(length))


def prepare_insert(collection: dict, index: str) -> Optional[dict[str, Any]]:
    if collection['metadata']['members_count'] > 10000:
        # print('Skipping collection with more than 10000 members')
        return None

    es_id = generate_id()

    collection['template']['nonavailable_members_count'] += 1
    collection['template']['invalid_members_count'] += 1

    return {
        '_op_type': 'create',
        '_index': index,
        '_id': es_id,
        '_source': collection
    }


def prepare_update(
        es_id: str,
        old_collection: dict,
        collection: dict,
        fields: list[str],
        index: str
) -> Optional[dict[str, Any]]:

    if collection['metadata']['members_count'] > 10000:
        return None

    # build the body of the update request
    doc = dict()

    # check if the fields have changed (the fields are separated by dots)
    for field in fields:
        old_value = get_nested_field(old_collection, field)
        new_value = get_nested_field(collection, field)

        # if both values are dicts, we can update only the changed fields
        if isinstance(old_value, dict) and isinstance(new_value, dict):
            old_keys = set(old_value.keys())
            new_keys = set(new_value.keys())

            # if some keys are missing, update the whole dict
            if old_keys - new_keys:
                set_nested_field(doc, field, new_value)
            else:
                # check if the new values are different or not present at all
                for key in new_keys:
                    if key not in old_keys or old_value[key] != new_value[key]:
                        set_nested_field(doc, field + '.' + key, new_value[key])

        # if the values are totally different, update the whole field
        elif old_value != new_value:
            set_nested_field(doc, field, new_value)

    # this update happens only in populate.py, so it is not presented in the previous JSONL file
    if 'template' in doc:
        if 'nonavailable_members_count' in doc['template']:
            doc['template']['nonavailable_members_count'] += 1
        if 'invalid_members_count' in doc['template']:
            doc['template']['invalid_members_count'] += 1

    # if no fields have changed, do nothing
    if not doc:
        return None

    return {
        '_op_type': 'update',
        '_index': index,
        '_id': es_id,
        'doc': doc
    }


def prepare_full_update(
        es_id: str,
        collection: dict,
        fields: list[str],
        index: str
) -> Optional[dict[str, Any]]:

    if collection['metadata']['members_count'] > 10000:
        return None

    # build the body of the update request
    doc = dict()

    # check if the fields have changed (the fields are separated by dots)
    for field in fields:
        value = get_nested_field(collection, field)
        set_nested_field(doc, field, value)

    # this update happens only in populate.py, so it is not presented in the previous JSONL file
    if 'template' in doc:
        if 'nonavailable_members_count' in doc['template']:
            doc['template']['nonavailable_members_count'] += 1
        if 'invalid_members_count' in doc['template']:
            doc['template']['invalid_members_count'] += 1

    # if no fields have changed, do nothing
    if not doc:
        return None

    return {
        '_op_type': 'update',
        '_index': index,
        '_id': es_id,
        'doc': doc
    }


def produce_update_operations(previous: str, current: str, output: str):
    es = connect_to_elasticsearch(CONFIG.elasticsearch)
    ids_mapping = collect_ids_mapping(es, CONFIG.elasticsearch.index)
    covered_ids = set()

    jsonl_index = JSONLIndex(previous, COMPARING_FIELDS, ids_mapping)

    print('Building index...')
    t0 = time.perf_counter()
    jsonl_index.build()
    print(f'Index built in {time.perf_counter() - t0:.2f} seconds.')

    print('Producing Elasticsearch update operations...')
    t0 = time.perf_counter()

    with jsonlines.open(current, 'r') as reader, jsonlines.open(output, 'w') as writer:
        for collection in tqdm.tqdm(reader):
            collection_id = collection['metadata']['id']
            covered_ids.add(collection_id)

            # we insert only if the collection is not in the Elasticsearch index
            # disregarding the fact that it might be in the previous JSONL file (insertion might have failed)
            # it also prevents from inserting duplicates (but mapping should be updated each time we run this script)
            if collection_id not in jsonl_index.ids_mapping:
                operation = prepare_insert(collection, CONFIG.elasticsearch.index)
                if operation is not None:
                    writer.write(operation)
                continue

            # since this collection is in the Elasticsearch index, but not in the previous JSONL file,
            # then it has either been inserted in this run, and something has failed later, or it has been
            # long before, and in the previous run, or even sooner, it has been marked as archived
            if collection_id not in jsonl_index.id2hash:
                operation = prepare_full_update(jsonl_index.ids_mapping[collection_id],
                                                collection,
                                                UPDATING_FIELDS,
                                                CONFIG.elasticsearch.index)
                if operation is not None:
                    writer.write(operation)
                continue

            # if the collection is in both the Elasticsearch index and the previous JSONL file,
            # then we calculate the differences between the two versions and update only the changed fields
            previous_hash = jsonl_index.get_hash(collection_id)
            current_hash = jsonl_index.hash(collection)

            if previous_hash == current_hash:
                continue

            es_id, old_collection = jsonl_index.get_document(collection_id)

            operation = prepare_update(es_id, old_collection, collection, UPDATING_FIELDS, CONFIG.elasticsearch.index)
            if operation is not None:
                writer.write(operation)

        print(f'Elasticsearch update operations produced in {time.perf_counter() - t0:.2f} seconds.')

        print('Setting archived flag for collections that are not in the input file...')
        t0 = time.perf_counter()
        for collection_id, es_id in tqdm.tqdm(ids_mapping.items(), total=len(ids_mapping)):
            if collection_id not in covered_ids:
                doc = {'data': {'archived': True}}
                writer.write({'_op_type': 'update', '_index': CONFIG.elasticsearch.index, '_id': es_id, 'doc': doc})

        print(f'Archived flag set in {time.perf_counter() - t0:.2f} seconds.')


def apply_operations(operations: str):
    es = connect_to_elasticsearch(CONFIG.elasticsearch)
    conflict_ids = set()
    with jsonlines.open(operations, 'r') as reader:
        progress = tqdm.tqdm(unit="actions")
        successes = 0

        for ok, action in streaming_bulk(client=es,
                                         index=CONFIG.elasticsearch.index,
                                         actions=reader,
                                         max_chunk_bytes=1_000_000,  # 1mb
                                         raise_on_error=False,
                                         raise_on_exception=False,
                                         max_retries=1):
            progress.update(1)
            successes += ok

            if not ok:
                print(action)
                if 'create' in action and action['create']['status'] == 409:
                    conflict_ids.add(action['create']['_id'])
                    print('Conflict id', action['create']['_id'])

    if not conflict_ids:
        print('No conflicts encountered.')
        return

    print('Resolving conflicts...')
    print(conflict_ids)

    with jsonlines.open(operations, 'r') as reader:
        for op in reader:
            if op['_op_type'] == 'create' and op['_id'] in conflict_ids:
                ok = False
                source = op['_source']
                while not ok:
                    substitute_id = generate_id()
                    try:
                        es.create(index=CONFIG.elasticsearch.index, id=substitute_id, body=source)
                        ok = True
                    except ConflictError:
                        ok = False
                        print(f'Conflict again for {source["template"]["collection_wikidata_id"]} with {substitute_id}')


def symlink(src: str, dst: str, force: bool = True):
    if force and os.path.exists(dst):
        os.remove(dst)
    os.symlink(src, dst)


def archive_merged_final(original: str, latest: str, archived: str):
    # TODO upload in S3 (this should be added with all the other S3 uploads)
    shutil.copy(original, archived)
    symlink(os.path.relpath(archived, os.path.dirname(latest)), latest)


with DAG(
        "update-es",
        default_args={
            "email": [CONFIG.email],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "cwd": CONFIG.local_prefix,
            "start_date": CONFIG.start_date,
        },
        description="Tasks related fetching the current index status, computing differences and updating the index. "
                    "In the end, it prepares the required files for the next run.",
        schedule=[MERGED_FINAL],
        start_date=CONFIG.start_date,
        catchup=False,
        tags=["update-es", "collection-templates"],
) as dag:
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
    produce_update_operations_task.doc_md = dedent(
        """\
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
    apply_operations_task.doc_md = dedent(
        """\
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
            "archived": MERGED_FINAL.local_name(
                prefix=f'archived_{CONFIG.elasticsearch.index}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S_")}'
            )
        },
    )
    archive_merged_final_task.doc_md = dedent(
        """\
    #### Task Documentation
    Archive current merged final, and update a symlink to it, so that it can be used
    as a reference for computing differences in the next run.
    """
    )

    produce_update_operations_task >> apply_operations_task >> archive_merged_final_task
