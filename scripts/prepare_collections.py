import csv
from argparse import ArgumentParser

import jsonlines as jsonlines
from tqdm import tqdm

from scripts.functions import WikiAPI

if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare collections for ElasticSearch.')
    parser.add_argument('input', help='JSONL file with validated category/list members')
    parser.add_argument('qrank', help='CSV from https://qrank.wmcloud.org/')
    parser.add_argument('output', help='JSONL file with collections')
    parser.add_argument('-n', default=None, type=int, help='number of collections to read for progress bar')

    args = parser.parse_args()

    ranks = {}
    with open(args.qrank, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for id, rank in tqdm(reader):
            ranks[id] = int(rank)

    wiki_api = WikiAPI()

    with jsonlines.open(args.input) as reader, jsonlines.open(args.output, mode='w') as writer:
        for obj in tqdm(reader, total=args.n):
            collection_item = obj['item']
            collection_type = obj['type']
            collection_article = obj['article']
            members = obj['members']
            valid_members_count = obj['valid_members_count']
            invalid_members_count = obj['invalid_members_count']

            collection_rank = ranks.get(collection_item, 1)  # rank_feature must be positive

            collection_name = wiki_api.curate_name(collection_article)
            collection_members = wiki_api.curate_members(members)

            if collection_members:
                writer.write({
                    'data': {  # owner controlled
                        'collection_name': collection_name,
                        'names': [{
                            'normalized_name': member.curated,
                            'avatar_override': '',
                            'tokenized_name': member.tokenized,
                            'system_interesting_score': None,  # TODO NOT owner controlled
                            'rank': None,  # TODO NOT owner controlled
                            'cached_status': None,  # TODO NOT owner controlled
                            'translations_count': None,  # TODO NOT owner controlled
                        } for member in collection_members],  # TODO sort
                        'collection_description': '',
                        'collection_keywords': [],
                        'collection_image': '',
                        'public': True,  # public or private collection

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
                        'id': '',  # UUID
                        'type': 'template',
                        'version': 0,
                        'owner': '',
                        'created': '',
                        'modified': '',
                        'votes': [],  # This could be some array of all the accounts that "upvoted" the collection.
                        'duplicated-from': '',
                        # a pointer to another collection. This field could be set whenever we create a collection from a template (reference back to origin template) or it could be set whenever a user 'duplicates' another user generated collection.
                        'members_count': len(collection_members),
                    },
                    'template': {  # template generator controlled
                        'collection_wikipedia_link': collection_article,
                        # link to category or "list of" article: https://en.wikipedia.org/wiki/List_of_sovereign_states or https://en.wikipedia.org/wiki/Category:Dutch_people
                        'collection_wikidata_id': collection_item,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q11750): Q11750
                        'collection_type_wikidata_id': collection_type,
                        # part of Wikidata url (http://www.wikidata.org/entity/Q3624078): Q3624078
                        'collection_articles': members,
                        'collection_rank': collection_rank,
                        # score based on popularity of category or "list of" article
                        'redirects_count': None,  # TODO
                        'translations_count': None,  # TODO
                        'has_related': None,  # has related category/list # TODO

                        # below metrics calculated on members
                        'members_rank_mean': None,  # TODO
                        'members_rank_median': None,  # TODO
                        'members_system_interesting_score_mean': None,  # TODO
                        'members_system_interesting_score_median': None,  # TODO
                        'valid_members_count': valid_members_count,
                        'invalid_members_count': invalid_members_count,
                        'valid_members_ratio': valid_members_count / (
                                    valid_members_count + invalid_members_count) if valid_members_count + invalid_members_count > 0 else 0.0,
                        'nonavailable_members': None,  # TODO
                    },
                    'name_generator': {  # Lambda NameGenerator preprocessor controlled

                    },
                })
