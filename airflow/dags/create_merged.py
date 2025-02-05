from textwrap import dedent
from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from collections import defaultdict

import multiprocessing
from functools import wraps
import jsonlines, regex, math, csv, random, time, sys, re, os
import numpy as np
from tqdm import tqdm
from rocksdict import AccessType, Rdict
from ens_normalize import DisallowedSequence, ens_cure
from namerank.namerank import NameRank
from urllib.parse import unquote
from unidecode import unidecode
from label_inspector.common import myunicode
from wikimapper.mapper import WikiMapper
from ens.utils import Web3
from ens.constants import EMPTY_SHA3_BYTES
from hexbytes import HexBytes

from create_collections import extract_article_name, VALIDATED_LIST_MEMBERS, VALIDATED_CATEGORY_MEMBERS
from create_kv import ROCKS_DB_5, ROCKS_DB_4
from create_inlets import CONFIG, WIKIMAPPER, QRANK, CollectionDataset, SUGGESTABLE_DOMAINS, AVATAR_EMOJI, upload_s3_file


INTERESTING_SCORE_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}interesting-score.rocks")
FORCE_NORMALIZE_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}force-normalize.rocks")
NAME_TO_HASH_CACHE = CollectionDataset(f"{CONFIG.remote_prefix}name-to-hash.rocks")
UNIQ_LIST_MEMBERS = CollectionDataset(f"{CONFIG.remote_prefix}uniq_list_members.txt")
UNIQ_CATEGORY_MEMBERS = CollectionDataset(f"{CONFIG.remote_prefix}uniq_category_members.txt")
LIST_MEMBERS_ALL_INFO = CollectionDataset(f"{CONFIG.remote_prefix}list_members_all_info.jsonl")
CATEGORY_MEMBERS_ALL_INFO = CollectionDataset(f"{CONFIG.remote_prefix}category_members_all_info.jsonl")
MERGED_COLLECTIONS = CollectionDataset(f"{CONFIG.remote_prefix}merged.jsonl") 
WITHOUT_LETTERS = CollectionDataset(f"{CONFIG.remote_prefix}merged-without-letters.jsonl") 
WITHOUT_DUPLICATES = CollectionDataset(f"{CONFIG.remote_prefix}merged-without-duplicates.jsonl") 
MERGED_FINAL = CollectionDataset(f"{CONFIG.remote_prefix}merged_final.jsonl")

MIN_VALUE = 1e-8


def memoize_ram(original_function=None, path=None):
    if path is None:
        path = f'cache-{original_function.__name__}.rocks'

    try:
        cache = Rdict(path)
    except Exception as e:
        print(e)
        cache = Rdict(path, access_type=AccessType.read_only())

    cache2 = {}

    def _decorate(function):

        @wraps(function)
        def wrapper(*args, **kwargs):

            try:
                return cache2[args[0]]
            except KeyError:
                try:
                    result = cache[args[0]]
                    cache2[args[0]] = result
                    return result
                except KeyError:
                    result = function(*args, **kwargs)
                    cache2[args[0]] = result
                    cache[args[0]] = result
                    return result

        return wrapper

    if original_function:
        return _decorate(original_function)

    return _decorate


def configure_interesting_score(namerank: NameRank, path):
    @memoize_ram(path=path)
    def get_interesting_score(label):
        r = namerank.inspect_label(label)
        try:
            interesting_score = r.namerank.interesting_score
            tokenizations = r.namerank.tokenizations
            try:
                best_tokenization = [token['token'] for token in tokenizations[0]['tokens']]
            except:
                best_tokenization = []
            return interesting_score, best_tokenization
        except:
            return None, []

    return get_interesting_score


def compute_unique_members(input, output, force_normalize_path):
    unique_members = set()

    force_normalize_function = configure_force_normalize(force_normalize_path)
    with jsonlines.open(input) as reader:
        for obj in tqdm(reader):
            members = [x[1] for x in obj['members']]

            collection_members = curate_members(members, force_normalize_function)
            unique_members.update([member.curated for member in collection_members])

    print(len(unique_members))

    with open(output, "w") as f:
        for member in unique_members:
            f.write(member + "\n")


def cache_interesting_score(list_members, category_members, interesting_score_path):
    namerank = NameRank()
    interesting_score_function = configure_interesting_score(namerank, interesting_score_path)

    unique_members = set()
    with open(list_members) as f:
        for line in f:
            unique_members.add(line.strip())

    with open(category_members) as f:
        for line in f:
            unique_members.add(line.strip())

    #with multiprocessing.Pool(8) as p:
    #    r = list(tqdm(p.imap(interesting_score_function, unique_members), total=len(unique_members)))
    for member in tqdm(unique_members):
        interesting_score_function(member)


class Member:
    def __init__(self, curated, tokenized):
        self.curated: str = curated
        self.tokenized = tokenized
        self.interesting_score: float | None = None
        self.rank: int | None = None
        self.status: str | None = None

    def json(self):
        return {
            'curated': self.curated,
            'tokenized': self.tokenized,
            'interesting_score': self.interesting_score,
            'rank': self.rank,
            'status': self.status,
        }

    @classmethod
    def from_dict(cls, member_data):
        member = cls(member_data['curated'], member_data['tokenized'])
        member.interesting_score = member_data['interesting_score']
        member.rank = member_data['rank']
        member.status = member_data['status']
        return member


def configure_force_normalize(cache_path):
    @memoize_ram(path=cache_path)
    def force_normalize(member):
        # member = member.replace('.', '')
        # TODO: remove () or use labels, e.g. Mary Poppins (film)
        # member = regex.sub(' *\(.*\)$', '', member)

        curated_token = ens_cure(member)
        curated_token2 = curated_token.replace('-', '')  # because other hyphens may be mapped
        curated_token2 = curated_token2.replace("'", '')

        curated_token3 = ''.join(
            [unidecode(c, errors='ignore') if myunicode.script_of(c) == 'Latin' else c for c in curated_token2])

        curated_token2 = curated_token3

        if curated_token2 != curated_token:
            curated_token2 = ens_cure(curated_token2)

        return curated_token2

    return force_normalize


def curate_member(member: str, force_normalize_function) -> Member:
    member = unquote(member)
    member = member.replace('.', '')
    member = member.replace('-', '')
    member = member.replace("'", '')
    member = member.replace('"', '')
    member = regex.sub(' *\(.*\)$', '', member)
    try:
        curated = force_normalize_function(member)
        tokenized = []
        for token in member.split(' '):
            try:
                curated_token = force_normalize_function(token)

                tokenized.append(curated_token)
            except DisallowedSequence as e:
                pass

        if len(curated) >= 3:
            return Member(curated, tokenized)
    except DisallowedSequence as e:
        print(member, e)
        return None


def curate_members(members: list[str], force_normalize_function) -> list[Member]:
    curated_members = []

    for member in members:
        member = curate_member(member, force_normalize_function)
        if member:
            curated_members.append(member)

    return curated_members


with DAG(
    "interesting-score-cache",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related to the creation of first stage of cache.",
    schedule=[VALIDATED_LIST_MEMBERS, VALIDATED_CATEGORY_MEMBERS],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["normalize", "collection-templates"],
) as dag:
    create_list_members_task = PythonOperator(
        task_id='create-unique-list-members',
        python_callable=compute_unique_members,
        op_kwargs={
            "input": VALIDATED_LIST_MEMBERS.local_name(), 
            "force_normalize_path": FORCE_NORMALIZE_CACHE.local_name(), 
            "output": UNIQ_LIST_MEMBERS.local_name(), 
        },
    )
    create_list_members_task.doc_md = dedent(
        """\
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
    create_category_members_task.doc_md = dedent(
        """\
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
    create_cache_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create cache for normalized names.
    """
    )

    create_list_members_task >> create_category_members_task >> create_cache_task


class Collection:
    def __init__(self):
        self.item = None
        self.types = None
        self.article = None
        self.name = None
        self.members: list[Member] | None = None
        self.valid_members_count = None
        self.invalid_members_count = None
        self.keywords = []
        self.description = None
        self.image = None
        self.page_banner = None
        self.rank = None
        self.is_merged = False

    def json(self):
        return {
            'item': self.item,
            'types': self.types,
            'article': self.article,
            'name': self.name,
            'members': [member.json() for member in self.members],
            'valid_members_count': self.valid_members_count,
            'invalid_members_count': self.invalid_members_count,
            'keywords': self.keywords,
            'description': self.description,
            'image': self.image,
            'page_banner': self.page_banner,
            'rank': self.rank,
            'is_merged': self.is_merged,
        }

    @classmethod
    def from_dict(cls, data):
        collection = cls()

        collection.item = data['item']
        collection.types = [tuple(t) for t in data['types']]
        collection.article = data['article']
        collection.name = data['name']
        collection.members = [Member.from_dict(member_data) for member_data in data['members']]
        collection.valid_members_count = data['valid_members_count']
        collection.invalid_members_count = data['invalid_members_count']
        collection.keywords = data['keywords']
        collection.description = data['description']
        collection.image = data['image']
        collection.page_banner = data['page_banner']
        collection.rank = data['rank']
        try:
            collection.is_merged = data['is_merged']
        except KeyError:
            pass
        return collection


def strip_eth(name: str) -> str:
    return name[:-4] if name.endswith('.eth') else name


def read_csv_domains(path: str) -> dict[str, str]:
    domains: dict[str, str] = {}
    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in tqdm(reader, desc='Reading domains'):
            assert len(row) == 3
            name, interesting_score, status = row
            name = strip_eth(name)
            domains[name] = status

    return domains


def uniq_members(collection_members):
    seen = set()
    for member in collection_members:
        if member.curated not in seen:
            seen.add(member.curated)
            yield member


def curate_name(collection_article: str):
    name = extract_article_name(collection_article)
    name = name.replace('_', ' ')
    name = unquote(name)
    name = regex.sub('^List of ', '', name)
    name = regex.sub('^Category:', '', name)
    name = name[0].upper() + name[1:]
    return name


def compute_all_info(input, output, interesting_score_path, qrank_path, domains_path, auxiliary_data_path, wikimapper_path, force_normalize_path):
    print(f"Current working directory: {os.getcwd()}")
    print(f"Attempting to open RocksDB at: {auxiliary_data_path}")
    try:
        auxiliary_data_db = Rdict(auxiliary_data_path, access_type=AccessType.read_only())
    except Exception as e:
        print(f"Failed to open RocksDB at {auxiliary_data_path}: {e}")
        print(f"Directory contents: {os.listdir(os.path.dirname(auxiliary_data_path))}")
        raise

    namerank = NameRank()
    mapper = WikiMapper(wikimapper_path)

    ranks = {}
    with open(qrank_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for id, rank in tqdm(reader, desc='Reading qrank'):
            ranks[id] = int(rank)

    domains = read_csv_domains(domains_path)
    interesting_score_function = configure_interesting_score(namerank, interesting_score_path)
    force_normalize_function = configure_force_normalize(force_normalize_path)

    # TODO add multiprocessing
    with jsonlines.open(input) as reader, jsonlines.open(output, mode='w') as writer:
        for obj in tqdm(reader):
            collection = Collection()
            collection.item = obj['item']
            collection.types = []
            for type in obj['type']:
                try:
                    collection.types.append((type, auxiliary_data_db[type]['label']))
                except KeyError:
                    # print(type, None)
                    collection.types.append((type, None))

            collection.article = obj['article']
            members = obj['members']
            collection.valid_members_count = obj['valid_members_count']
            collection.invalid_members_count = obj['invalid_members_count']
            collection.rank = ranks.get(collection.item, 0)

            collection.name = curate_name(collection.article)
            # print(collection_article, collection_name)
            # alwats both name and label are present, only few items have only description
            if collection.item in auxiliary_data_db:
                info_data = auxiliary_data_db[collection.item]
                try:
                    wikidata_label = info_data['label']
                    # wikidata_name = info_data['name']
                    collection.keywords.append(wikidata_label)
                except KeyError:
                    print('No label/name', info_data)

                try:
                    wikidata_description = info_data['description']
                    if wikidata_description not in ['Wikimedia list article', 'Wikimedia category']:
                        # print('Description', collection_article, collection_item, wikidata_description)
                        wikidata_description = regex.sub('^[Ww]ikimedia ', '', wikidata_description)
                        collection.description = wikidata_description
                except KeyError:
                    pass

                try:
                    collection.images = [unquote(url) for url in info_data['image']]
                except KeyError:
                    pass

                try:
                    collection.page_banners = [unquote(url) for url in info_data['page_banner']]
                except KeyError:
                    pass

            else:
                print('No item in auxiliary data db', collection.item)

            # add keywords
            collection.keywords = mapper.id_to_titles(collection.item)
            collection.keywords = [curate_name(k) for k in collection.keywords]
            collection.keywords = [k for k in collection.keywords if k != collection.name]
            collection_members = []
            for member_id, member_name in members:
                member = curate_member(member_name, force_normalize_function)

                if member is None:
                    if member_id in auxiliary_data_db:
                        info_data = auxiliary_data_db[member_id]
                        try:
                            wikidata_label = info_data['label']
                            member = curate_member(wikidata_label, force_normalize_function)
                        except KeyError:
                            print('No label/name', info_data)

                if member:
                    collection_members.append(member)

                    member.interesting_score = interesting_score_function(member.curated)[0]
                    member.rank = ranks.get(member_id, 0)
                    member.status = domains.get(member.curated, None)


            collection.members = uniq_members(sorted(collection_members, 
                                                     key=lambda m: math.log(m.rank + 1, 2) / max(len(m.curated), 10), 
                                                     reverse=True))

            writer.write(collection.json())


with DAG(
    "collections-all-info",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related final processing of list members.",
    schedule=[INTERESTING_SCORE_CACHE, VALIDATED_CATEGORY_MEMBERS, VALIDATED_LIST_MEMBERS, WIKIMAPPER, ROCKS_DB_5, QRANK, SUGGESTABLE_DOMAINS],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["rank", "domains", "normalize", "collection-templates", "lists", "categories"],
) as dag:
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
    create_list_members_final_task.doc_md = dedent(
        """\
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
    create_category_members_final_task.doc_md = dedent(
        """\
    #### Task Documentation
    Create file with list members supplemented with the final information.
    """
    )

    create_list_members_final_task >> create_category_members_final_task


def merge_collections(collection1: Collection, collection2: Collection) -> Collection:
    if int(collection2.item[1:]) < int(collection1.item[1:]):  # smaller id as collection stable id
        collection1.item = collection2.item

    collection1.members.extend(collection2.members)
    collection1.members = sorted(collection1.members, key=lambda x: x.rank, reverse=True)
    collection1.members = list(uniq_members(collection1.members))

    collection1.valid_members_count += collection2.valid_members_count
    collection1.invalid_members_count += collection2.invalid_members_count

    collection1.keywords.extend(collection2.keywords)
    collection1.types = list(set(collection1.types + collection2.types))

    if not collection1.description:
        collection1.description = collection2.description

    if not collection1.image:
        collection1.image = collection2.image

    if not collection1.page_banner:
        collection1.page_banner = collection2.page_banner

    collection1.rank = max(collection1.rank, collection2.rank)

    collection1.is_merged = True

    return collection1


filter_types = {
    'Q11266439',  # Wikimedia template 17183
    'Q4663261',  # Wikipedia:Stub 14390 # TODO all have wrong type causing valid members = 0 
    'Q13406463',  # Wikimedia list article 4181
    'Q11753321',  # Wikimedia navigational template 932
    'Q20769160',  # Wikimedia userbox template 759
    'Q30432511',  # Wikimedia meta category 5
    'Q4167836',  # Wikimedia category 203
    'Q33532284',  # Wikimedia list of lists
}


def should_filter_by_type(collection):
    return set([type[0] for type in collection.types]) & filter_types


filter_name_regexes = (
    r'Wikipedia:.*',
    r'Highways numbered .*',
    r'Lists (of|that) .*',
    r'Incomplete lists? from .*',
)


def should_filter_by_regex(collection):
    return re.match('|'.join(filter_name_regexes), collection.name)


def should_filter_by_by(collection):
    m = regex.search(' by ([^ ]*)', collection.name)
    if not m: return False
    by_what = m.group(1)
    return by_what[0].islower()


def merge_lists_and_categories(output, list_members, category_members, related_data_path):
    related_data_db = Rdict(related_data_path, access_type=AccessType.read_only())

    count_lists = count_categories = count_written = 0
    count_merges = count_filtered_by_type = count_merges_by_name = count_filtered_by_regex = count_filtered_by_by = 0

    categories_related_to_list = {}
    lists = {}
    list_names = {}

    with jsonlines.open(output, mode='w') as writer:
        with jsonlines.open(list_members) as reader:
            for obj in tqdm(reader, desc='Reading lists'):
                count_lists += 1
                collection = Collection.from_dict(obj)

                if should_filter_by_type(collection):
                    count_filtered_by_type += 1
                    continue

                if should_filter_by_regex(collection):
                    count_filtered_by_regex += 1
                    continue

                if should_filter_by_by(collection):
                    count_filtered_by_by += 1
                    continue

                lists[collection.item] = collection
                list_names[collection.name] = collection
                try:
                    same_as = related_data_db[collection.item]['category_related_to_list']
                    # print(f'{collection.item} -> {same_as}')
                    for category_wikidata_id in same_as:
                        categories_related_to_list[category_wikidata_id] = collection
                except KeyError:
                    pass
            print(f'Categories related to list: {len(categories_related_to_list)}')

        with jsonlines.open(category_members) as reader:
            for obj in tqdm(reader, desc='Reading categories'):
                count_categories += 1
                collection = Collection.from_dict(obj)

                if should_filter_by_type(collection):
                    count_filtered_by_type += 1
                    continue

                if should_filter_by_regex(collection):
                    count_filtered_by_regex += 1
                    continue

                if should_filter_by_by(collection):
                    count_filtered_by_by += 1
                    continue

                # TRY MERGE BY TYPE
                related_lists = []
                if collection.item in categories_related_to_list:
                    related_lists.append(categories_related_to_list[collection.item])
                try:
                    same_as = related_data_db[collection.item]['list_related_to_category']
                    for list_wikidata_id in same_as:
                        try:
                            related_lists.append(lists[list_wikidata_id])
                        except KeyError:
                            pass
                except KeyError:
                    pass

                merged = False
                for list_collection in related_lists:
                    merge_collections(list_collection, collection)
                    count_merges += 1
                    merged = True
                    break

                if merged: continue

                # TRY MERGE BY NAME
                if collection.name in list_names:
                    merge_collections(list_names[collection.name], collection)
                    count_merges_by_name += 1
                    merged = True

                # WRITE NOT MERGED
                if not merged:
                    writer.write(collection.json())
                    count_written += 1

        for list_collection in tqdm(lists.values(), desc='Writing lists'):
            writer.write(list_collection.json())
            count_written += 1

    print(f'All collections: {count_lists + count_categories}')
    print(f'Lists: {count_lists}, Categories: {count_categories}, Written {count_written}')
    print(f'Merged by type {count_merges} categories into lists')
    print(f'Merged by name {count_merges_by_name} categories into lists')
    print(f'Filtered by type: {count_filtered_by_type}')
    print(f'Filtered by regex: {count_filtered_by_regex}')
    print(f'Filtered by by: {count_filtered_by_by}')


explicit_parentheses_patterns = [
    r'[Ll]isted [Aa]lphabetically',
    r'[Ll]ist',
    r'[Cc]urrent',
    r'[Cc]hronological',
    r'[Cc]ategorised',
    r'by .*?',
    r'[Aa]lphabetical',
    r'[Aa]lphabetic',
    r'[Ss]eat .*?',
    r'[Pp]art .*?',
    r'MONA .*?',
    r'[Cc]onstituencies .*?',
    r'!\$@',
    r'[A-Z][a-z]',
]


explicit_normalization_patterns = [
    r'(?P<stripped>.* lists? of )(?P<normalized>\w.*)',
    r'(?P<stripped>.*(?<!and )(?<!are )\b[Ll]isted )(?P<normalized>.*?(buildings|churches|lighthouses|memorials).*)',
]


alphabet_range_pattern = r'^[^A-Za-z]*?\b[A-Za-z]( ?[-–] ?[A-Za-z])?\b[^A-Za-z]*$'


# TODO move this to a separate class with the whole pipeline defined
def remove_collections_with_letters(input, output):
    count_matches = count_merged = count_normalized = 0

    with jsonlines.open(output, mode='w') as writer:
        to_merge = defaultdict(list)
        matches = defaultdict(list)
        stripped = defaultdict(list)
        with jsonlines.open(input) as reader:
            for obj in tqdm(reader, desc='Reading collections'):
                name = obj['name']
                # grep -E "([,:–] [A-Z0-9]+[a-z]* ?([–-]| to ) ?[^ ]+\"$)|((:|,|–|starting with) [A-Z]\"$)" names.txt | sort | less | wc -l
                m1 = regex.search(r'(?P<normalized>.*)(?P<stripped>([,:–(] ?[A-Z0-9]+[a-z]* ?([–-]| to ) ?[^ ]+$)|((: |, |– |starting with |\()[A-Z]\)?$))', name)
                m2 = regex.search(r'(?P<normalized>.*\S)(?P<stripped>\s*\((' + '|'.join(explicit_parentheses_patterns) + r')\))$', name)
                m3 = regex.search('|'.join(explicit_normalization_patterns), name)

                if m := m1 or m2 or m3:
                    count_matches += 1

                    normalized_name = m.group('normalized').strip()
                    if normalized_name[0].islower():
                        normalized_name = normalized_name[0].upper() + normalized_name[1:]
                    stripped_part = m.group('stripped')

                    to_merge[normalized_name].append(Collection.from_dict(obj))
                    matches[normalized_name].append((m1 is not None, m2 is not None, m3 is not None))
                    stripped[normalized_name].append(stripped_part)
                    print(f'{name} -> {[normalized_name, stripped_part]}')
                else:
                    writer.write(obj)

        for normalized_name, collections in to_merge.items():
            m1, m2, m3 = map(any, zip(*matches[normalized_name])) \
                if normalized_name in matches else (False, False, False)
            # normalizing if we can merge multiple collections
            if len(collections) > 1:
                print(f'Merging {normalized_name}')
                merged = collections[0]
                for collection in collections[1:]:
                    merged = merge_collections(merged, collection)
                merged.name = normalized_name
                writer.write(merged.json())
                count_merged += len(collections)
            # or if there was a match with explicit patterns
            elif m2 or m3 or (alphabet_range := re.match(alphabet_range_pattern, stripped[normalized_name][0])):
                print(f'Normalizing {collections[0].name} -> {normalized_name}')
                if alphabet_range:
                    print(f'Normalized alphabet range: {alphabet_range.group(0)}')
                    # zeroes out the match for cases where m2 or m3 is True and alphabet range is not updated
                    alphabet_range = None

                normalized = collections[0]
                normalized.name = normalized_name
                writer.write(normalized.json())
                count_normalized += 1
            else:
                writer.write(collections[0].json())

    print(f'Matches: {count_matches}')
    print(f'Merged: {count_merged}')
    print(f'Normalized: {count_normalized}')


def label_to_hash(label: str) -> HexBytes:
    if "." in label:
        raise ValueError(f"Cannot generate hash for label {label!r} with a '.'")
    return Web3().keccak(text=label)


def configure_nomrmal_name_to_hash(path):
    @memoize_ram(path=path)
    def normal_name_to_hash(name: str) -> str:
        node = EMPTY_SHA3_BYTES
        if name:
            labels = name.split(".")
            for label in reversed(labels):
                labelhash = label_to_hash(label)
                assert isinstance(labelhash, bytes)
                assert isinstance(node, bytes)
                node = Web3().keccak(node + labelhash)
        return node.hex()

    return normal_name_to_hash


class AvatarEmoji:
    def __init__(self, path):
        self.load_emojis(path)

    def load_emojis(self, path):
        self.emojis = {}
        self.emoji_counts = {}
        
        with open(path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                type_id, type_name, category, count, *type_emojis = row
                if type_name == 'OTHER':
                    self.other_emojis = type_emojis
                    continue
                count = int(count)
                self.emojis[type_name] = type_emojis
                self.emoji_counts[type_name] = count

    def get_emoji(self, collection_id, types):
        #sort types by emoji count from the less popular
        types = sorted(types, key=lambda x: self.emoji_counts.get(x, sys.maxsize))
        random.seed(collection_id)
        if types and types[0] in self.emojis:
            return random.choice(self.emojis[types[0]])
        else:
            return random.choice(self.other_emojis)


def collection_factory(input, output, name_to_hash_path, avatar_path):
    namerank = NameRank()
    normal_name_to_hash_function = configure_nomrmal_name_to_hash(name_to_hash_path)

    current_time = time.time() * 1000

    avatar_emoji = AvatarEmoji(avatar_path)

    with jsonlines.open(input) as reader, jsonlines.open(output, mode='w') as writer:
        for obj in tqdm(reader):
            collection = Collection.from_dict(obj)

            collection.rank = max(collection.rank, 1)  # rank_feature must be positive

            status_counts = {'available': 0, 'taken': 0, 'on_sale': 0, 'recently_released': 0, 'never_registered': 0}
            if collection.members:
                for member in collection.members:
                    status = member.status
                    if status is None:
                        status = 'never_registered'
                    status_counts[status] += 1
                nonavailable_members = len(
                    [True for m in collection.members if m.status in ('taken', 'on_sale', 'recently_released')])

                # sort collection members
                collection.members = sorted(collection.members,
                                            key=lambda x: math.log(x.rank + 1, 2) / max(len(x.curated), 10),
                                            reverse=True)

                template_names = [{
                    'normalized_name': member.curated,
                    'tokenized_name': member.tokenized,
                    'system_interesting_score': member.interesting_score,
                    'rank': member.rank,
                    'cached_status': member.status,
                    'namehash': normal_name_to_hash_function(member.curated + '.eth'),
                    # 'translations_count': None,
                } for member in collection.members]

                random.seed(collection.item)
                banner_image_number = random.randint(0, 19)

                writer.write({
                    'data': {  # owner controlled
                        'collection_name': collection.name,
                        'names': [{
                            'normalized_name': member.curated,
                            'avatar_override': '',
                            'tokenized_name': member.tokenized,
                        } for member in collection.members],  # TODO sort
                        # 'collection_description': collection.description,
                        'collection_description': 'A collection of names auto-generated from Wikipedia and Wikidata using AI',
                        'collection_keywords': collection.keywords,
                        'collection_image': collection.image[0] if collection.image else None,  # TODO
                        'public': True,  # public or private collection

                        'banner_image': f'tc-{banner_image_number:02d}.png',
                        'avatar_image': None,
                        'avatar_emoji': avatar_emoji.get_emoji(collection.item, [type for cid, type in collection.types]),

                        'archived': False,
                        # By default false. This would be used exclusively for the narrowly defined purpose of deciding if we include a collection on a user's "Collections" tab in their "Account Page".
                    },
                    'curation': {  # admin controlled
                        'curated': False,  # manually curated by NameHash
                        'category': '',  # Each collection can optionally be curated into 0 or 1 predefined categories.
                        'trending': False,
                        # This is a boolean, false by default, that we might use to say a collection is trending that would give it more visibility on NameHash.
                        'community-choice': False,
                        # This is a boolean, false by default, that we might use to say a collection is trending that would give it more visibility on NameHash.

                    },
                    'metadata': {  # system controlled
                        'id': collection.item,  # UUID
                        'type': 'template',
                        'version': 0,
                        'owner': '0xcb8f5f88e997527d76401ce3df8c8542b676e149',
                        'created': current_time,
                        'modified': current_time,
                        'votes': [],  # This could be some array of all the accounts that "upvoted" the collection.
                        'duplicated-from': '',
                        # a pointer to another collection. This field could be set whenever we create a collection from a template (reference back to origin template) or it could be set whenever a user 'duplicates' another user generated collection.
                        'members_count': len(collection.members),
                        'collection_name_log_probability': namerank.nlp_inspector.ngrams.sequence_log_probability(
                            collection.name.lower().split(' ')),
                    },
                    'template': {  # template generator controlled
                        'collection_wikipedia_link': collection.article,
                        # link to category or "list of" article: https://en.wikipedia.org/wiki/List_of_sovereign_states or https://en.wikipedia.org/wiki/Category:Dutch_people
                        'collection_wikidata_id': collection.item,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q11750): Q11750
                        'collection_types': collection.types,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q3624078): Q3624078
                        # 'collection_articles': members,
                        'collection_rank': collection.rank,
                        # score based on popularity of category or "list of" article
                        'translations_count': None,  # TODO
                        'has_related': None,  # has related category/list # TODO

                        'collection_images': collection.image,
                        'collection_page_banners': collection.page_banner,

                        'names': template_names,
                        'top10_names': template_names[:10],
                        'top25_names': template_names[:25],
                        
                        # below metrics calculated on members
                        'members_rank_mean': max(np.mean([m.rank for m in collection.members]), MIN_VALUE),
                        'members_rank_median': max(np.median([m.rank for m in collection.members]), MIN_VALUE),
                        'members_system_interesting_score_mean': max(
                            np.mean([m.interesting_score for m in collection.members]), MIN_VALUE),
                        'members_system_interesting_score_median': max(
                            np.median([m.interesting_score for m in collection.members]), MIN_VALUE),
                        'valid_members_count': collection.valid_members_count,
                        'invalid_members_count': collection.invalid_members_count,
                        'valid_members_ratio': collection.valid_members_count / (
                                collection.valid_members_count + collection.invalid_members_count) if collection.valid_members_count + collection.invalid_members_count > 0 else 0.0,
                        'nonavailable_members_count': nonavailable_members,
                        'nonavailable_members_ratio': max(nonavailable_members / len(collection.members), MIN_VALUE),

                        'is_merged': collection.is_merged,
                        'available_count': status_counts['available'],
                        'taken_count': status_counts['taken'],
                        'on_sale_count': status_counts['on_sale'],
                        'recently_released_count': status_counts['recently_released'],
                        'never_registered_count': status_counts['never_registered'],
                    },
                    'name_generator': {  # Lambda NameGenerator preprocessor controlled

                    },
                })


def remove_duplicates(input, output):
    count_merged = 0
    with jsonlines.open(output, mode='w') as writer:
        collection_names_count = defaultdict(int)
        with jsonlines.open(input) as reader:
            for obj in tqdm(reader, desc='Reading collections'):
                name = obj['name']
                collection_names_count[name] += 1

        collection_names = defaultdict(list)
        with jsonlines.open(input) as reader:
            for obj in tqdm(reader, desc='Reading collections'):
                name = obj['name']
                if collection_names_count[name] > 1:
                    collection_names[name].append(Collection.from_dict(obj))
                else:
                    writer.write(obj)

        for collections in collection_names.values():
            merged = collections[0]
            for collection in collections[1:]:
                merged = merge_collections(merged, collection)
            writer.write(merged.json())
            count_merged += len(collections)

    print(f'Merged: {count_merged}')


with DAG(
    "collections-merge",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related merging of category and list members.",
    schedule=[LIST_MEMBERS_ALL_INFO, CATEGORY_MEMBERS_ALL_INFO, ROCKS_DB_4],
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["merge", "collection-templates"],
) as dag:
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
    merge_lists_and_categories_task.doc_md = dedent(
        """\
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

    upload_merged_task.doc_md = dedent(
        """\
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
    remove_letters_task.doc_md = dedent(
        """\
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
    remove_duplicates_task.doc_md = dedent(
        """\
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
    final_processing_task.doc_md = dedent(
        """\
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

    upload_final_task.doc_md = dedent(
        """\
    #### Task Documentation

    Upload final collection members data to S3.
    """
    )

    merge_lists_and_categories_task >> upload_merged_task >> remove_letters_task >> remove_duplicates_task >> final_processing_task >> upload_final_task
