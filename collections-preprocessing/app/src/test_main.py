from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_collections_heroes():
    input_json = {
        "data": [
            {
                "collection_name": "marvel heroes",
                "keywords": ["marvel", "superhero"],
                "names": [
                    "hulk", "CaptainAmerica", "deadpool", "doctorstrange", "president of america"
                ],
                "description": "marvel heroes doing epic things"
            },
            {
                "collection_name": "dc heroes",
                "keywords": ["dc", "superhero"],
                "names": [
                    "superwoman", "Batman", "wonderwoman"
                ],
                "description": "dc heroes doing epic things"
            }
        ]
    }

    response = client.post(
        '/collections',
        json=input_json
    )

    assert response.status_code == 200
    output_json = response.json()

    for key in {'collection_name', 'keywords', 'names', 'description'}:
        assert output_json['data'][0][key] == input_json['data'][0][key]
        assert output_json['data'][1][key] == input_json['data'][1][key]

    assert output_json['data'][0]['tokenized_names'] == \
           [['hulk'], ['captain', 'america'], ['dead', 'pool'], ['doctor', 'strange'], ['president', 'of', 'america']]

    assert output_json['data'][0]['tokenized_names_counts'] == {
        "hulk": 1,
        "captain": 1,
        "america": 2,
        "dead": 1,
        "pool": 1,
        "doctor": 1,
        "strange": 1,
        "president": 1,
        "of": 1
    }

    assert output_json['data'][1]['tokenized_names'] == \
           [['super', 'woman'], ['batman'], ['wonder', 'woman']]

    assert output_json['data'][1]['tokenized_names_counts'] == {
        "super": 1,
        "woman": 2,
        "batman": 1,
        "wonder": 1
    }


def test_empty():
    input_json = {
        "data": [
            {
                "collection_name": "empty",
                "keywords": [],
                "names": [],
                "description": ""
            }
        ]
    }

    response = client.post(
        '/collections',
        json=input_json
    )

    assert response.status_code == 200

    assert response.json() == {
        "data": [
            {
                "collection_name": "empty",
                "keywords": [],
                "names": [],
                "description": "",
                "tokenized_names": [],
                "tokenized_names_counts": {}
            }
        ]
    }
