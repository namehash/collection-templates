from pydantic import BaseModel, Field


class Collection(BaseModel):
    collection_name: str = Field(description='Collection name')
    keywords: list[str] = Field(default_factory=list, description='List of keywords relevant to collection')
    names: list[str] = Field(description='List of names included in collection')
    description: str = Field(default='', description='Collection description')

    class Config:
        schema_extra = {
            "example":
                {
                    "collection_name": "marvel heroes",
                    "keywords": ["marvel", "superhero"],
                    "names": [
                        "hulk", "CaptainAmerica", "deadpool", "doctorstrange", "president of america"
                    ],
                    "description": "marvel heroes doing epic things"
                }}


class AugmentedCollection(BaseModel):
    collection_name: str = Field(description='Collection name')
    keywords: list[str] = Field(default_factory=list, description='List of keywords relevant to collection')
    names: list[str] = Field(description='List of names included in collection')
    description: str = Field(default='', description='Collection description')
    # added fields:
    tokenized_names: list[list[str]] = Field(
        description="List of lists of tokens, ith sub-list contains ith's name tokens")
    tokenized_names_counts: dict[str, int] = Field(
        description='Dict mapping each token to its number of occurrences in collection')


class CollectionListPOSTRequest(BaseModel):
    data: list[Collection]


class AugmentedCollectionListPOSTResponse(BaseModel):
    data: list[AugmentedCollection]
