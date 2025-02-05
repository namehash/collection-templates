from datetime import datetime
import jsonlines
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


es = Elasticsearch(
    hosts=[{
        'scheme': os.getenv('ES_SCHEME'),
        'host': os.getenv('ES_HOST'),
        'port': int(os.getenv('ES_PORT'))
    }],
    http_auth=(os.getenv('ES_USERNAME'), os.getenv('ES_PASSWORD')),
    timeout=60,
    http_compress=True,
)

def export_to_jsonl():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'collection_templates_{timestamp}.jsonl'

    query = {
        'query': {
            'term': {
                'metadata.type': 'template'
            }
        },
    }
    
    try:
        with jsonlines.open(filename, 'w') as writer:
            print(f"Starting export to {filename}")
            count = 0

            # Scan through the index
            for doc in scan(es, index=os.getenv('ES_INDEX'), query=query):
                writer.write(doc)

                count += 1
                if count % 1000 == 0:
                    print(f"Exported {count} documents...")

        print(f"Export complete. Total documents: {count}")
        print(f"File saved as: {filename}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    export_to_jsonl()
