import wordninja
from collections import Counter
from functools import reduce

from model import Collection, AugmentedCollection


def preprocess_collections(collection_list: list[Collection]) -> list[AugmentedCollection]:

    augmented_collection_list = []

    for collection in collection_list:
        tokenized_names = [wordninja.split(name.lower()) for name in collection.names]
        tokenized_names_lengths = Counter(reduce(list.__add__, tokenized_names)) \
            if len(tokenized_names) != 0 else dict()
        augmented_collection_list.append(
            AugmentedCollection(
                collection_name=collection.collection_name,
                keywords=collection.keywords,
                names=collection.names,
                description=collection.description,
                tokenized_names=tokenized_names,
                tokenized_names_counts=dict(tokenized_names_lengths)
            )
        )

    return augmented_collection_list
