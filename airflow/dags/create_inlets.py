
from datetime import datetime, timedelta
from textwrap import dedent
from pathlib import Path
import boto3
import shutil
import re
import os
import airflow

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset
from airflow.models import Variable, DagModel, DagTag, TaskInstance
from airflow.models.dataset import DatasetDagRunQueue

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.dates import days_ago

from dataclasses import dataclass


@dataclass
class ElasticsearchConfig:
    scheme: str
    host: str
    port: int
    index: str
    username: str
    password: str


@dataclass
class Config:
    email: str
    local_prefix: str
    remote_prefix: str
    date_str: str
    start_date: datetime
    run_interval: timedelta
    elasticsearch: ElasticsearchConfig
    s3_bucket_upload: str
    s3_bucket_download: str


CONFIG = Config(
    email="apohllo@o2.pl",
    # FIXME move this to separate directory, so we don't mix files for different pipelines
    local_prefix="/home/airflow/data/",
    remote_prefix="file:///home/airflow/data/",
    date_str=datetime.now().strftime("%Y%m%d"),
    start_date=datetime(2021, 1, 1),
    run_interval=timedelta(weeks=4),
    elasticsearch=ElasticsearchConfig(
        scheme=Variable.get("es_scheme", "http"),
        host=Variable.get("es_host", "localhost"),
        port=int(Variable.get("es_port", "9200")),
        index=Variable.get("es_index", "collection-templates-1"),
        username=Variable.get("es_username", "elastic"),
        password=Variable.get("es_password", "changeme"),
    ),
    s3_bucket_upload='collection-templates-airflow-jiiylvkbwo',
    s3_bucket_download="prod-name-generator-namegeneratori-inputss3bucket-c26jqo3twfxy"
)


class CollectionDataset(Dataset):
    def name(self) -> str:
        return self.uri.split("/")[-1]

    def latest_name(self) -> str:
        return re.sub(f"-{datetime.now().strftime('%Y%m%d')}-", "-latest-", self.name())

    def current_name(self) -> str:
        return re.sub("-latest-", f"-{datetime.now().strftime('%Y%m%d')}-", self.name())

    def local_name(self, prefix: str = '') -> str:
        return CONFIG.local_prefix + prefix + self.name()

    def current_local_name(self) -> str:
        return CONFIG.local_prefix + self.current_name()

    def latest_local_name(self) -> str:
        return CONFIG.local_prefix + self.latest_name()

    # could be changed to remote name, but it is based on a file, so maybe it's not the best idea
    # to keep it as reference name for the dataset.
    def s3_name(self) -> str:
        if(os.path.exists(PROCESS_START_DATE.local_name())):
            with open(PROCESS_START_DATE.local_name()) as input:
                date = input.read().strip()
                return date + "/" + self.name()



PROCESS_START_DATE = CollectionDataset(f"{CONFIG.remote_prefix}start-date.txt")
WIKIDATA_FILTERED = CollectionDataset(f"{CONFIG.remote_prefix}latest-truthy.filtered.nt.bz2")
WIKIPEDIA_PAGELINKS = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-pagelinks.sql.gz")
WIKIPEDIA_REDIRECT = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-redirect.sql.gz")
WIKIPEDIA_CATEGORYLINKS = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-categorylinks.sql.gz")
WIKIPEDIA_PAGEPROPS = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-page_props.sql.gz")
WIKIPEDIA_PAGE = CollectionDataset(f"{CONFIG.remote_prefix}enwiki-latest-page.sql.gz")
WIKIMAPPER = CollectionDataset(f"{CONFIG.remote_prefix}index_enwiki-latest.db")
QRANK = CollectionDataset(f"{CONFIG.remote_prefix}qrank.csv")
SUGGESTABLE_DOMAINS = CollectionDataset(f"{CONFIG.remote_prefix}suggestable_domains.csv") 
AVATAR_EMOJI = CollectionDataset(f"{CONFIG.remote_prefix}avatars-emojis.csv")


def wget_for_wikidata(type: str):
    if 'AIRFLOW_TEST' in os.environ and os.environ['AIRFLOW_TEST'] == 'true':
        return f"'https://public-collection-templates-tests-picxcxdoqemigw.s3.amazonaws.com/latest-truthy.nt.bz2'"
    else:
        return f"https://dumps.wikimedia.org/wikidatawiki/entities/latest-{type}.nt.bz2"

def download_s3_file(bucket, remote_path, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket, remote_path, local_path)

def create_s3_directory(bucket, path):
    s3 = boto3.client('s3')
    if(not re.match(r"^.*/", path)):
        path += "/"
    s3.put_object(Bucket=bucket, Key=path)

def create_buckup_directory(bucket):
    start_date = CONFIG.start_date
    with open(PROCESS_START_DATE.local_name()) as input:
        start_date = input.read().strip()

    create_s3_directory(bucket, start_date + "/")

def upload_s3_file(bucket, local_path, remote_path):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, remote_path)

with DAG(
    "filter-wikidata",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="Tasks related to processing of source Wikidata files",
    schedule=[PROCESS_START_DATE],
    catchup=False,
    tags=["download", "filter", "collection-templates"],
    start_date=CONFIG.start_date,
) as dag:

    # P18       image
    # P1753     list related to category
    # P31       instance of
    # P1754     logo image
    # P4224     category contains 
    # P948      page banner 
    # P279      subclass of
    # P360      is list of
    # TODO skos:altLabel - check if is present in truthy

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

    dag.doc_md = """
    Downloading and filtering Wikidata for entries including only specific subjects and predicates.
    """  

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

def wget_for_wikipedia(dataset):
    return f"https://dumps.wikimedia.org/enwiki/latest/{dataset.name()} -O {dataset.local_name()}"

with DAG(
    "download-wikipedia",
    default_args={
        "cwd": CONFIG.local_prefix,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related to downloading Wikipedia source files",
    schedule=[PROCESS_START_DATE],
    catchup=False,
    tags=["download", "collection-templates"],
    max_active_tasks=2,
    start_date=CONFIG.start_date,
) as dag:

    dag.doc_md = """
    Downloading of Wikipedia dump files
    """  

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


with DAG(
    "wikimapper",
    default_args={
        "cwd": CONFIG.local_prefix,
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related to running the wikimapper service",
    catchup=False,
    tags=["db", "collection-templates"],
    schedule=[WIKIPEDIA_REDIRECT, WIKIPEDIA_PAGE, WIKIPEDIA_PAGEPROPS],
    start_date=CONFIG.start_date,
) as dag:
    dag.doc_md = """
    Loading of Wikipedia dump files to wikimapper database
    """  
    wikimapper_task = BashOperator(
        outlets=[WIKIMAPPER],
        task_id="load-redirect",
        bash_command=f"wikimapper create enwiki-latest --dumpdir {CONFIG.local_prefix} --target {WIKIMAPPER.name()}",
    )

    wikimapper_task.doc_md = dedent(
        """\
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

    upload_wikimapper_task.doc_md = dedent(
        """\
    #### Task Documentation

    Upload wikimapper data to S3.
    """
    )

    wikimapper_task >> upload_wikimapper_task

with DAG(
    "qrank",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related downloading of rank files.",
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["rank", "collection-templates"],
    schedule=[PROCESS_START_DATE],
) as dag:
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

with DAG(
    "namehash-files",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related downloading of domain files.",
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["domain", "collection-templates"],
    schedule=[PROCESS_START_DATE],
) as dag:
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
    domains_task.doc_md = dedent(
        """\
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
    avatar_task.doc_md = dedent(
        """\
    #### Task Documentation

    Download Avatar file from namehash bucket.
    """
    )


def remove_intermediate_files():
    for file in Path(CONFIG.local_prefix).glob('*'):
        if file.is_file() and not file.name.startswith('archived_'):
            file.unlink()

        if file.is_dir():
            shutil.rmtree(file)


@provide_session
def clear_dags(session=NEW_SESSION):
    with session:
        tags = session.query(DagTag).filter(DagTag.name == 'collection-templates').all()
        for tag in tags:
            dag = session.get(DagModel, tag.dag_id)

            runs = session.query(DatasetDagRunQueue).filter(DatasetDagRunQueue.target_dag_id == dag.dag_id).all()
            for run in runs:
                session.query(DatasetDagRunQueue).where(DatasetDagRunQueue.target_dag_id == run.target_dag_id, 
                                                                 DatasetDagRunQueue.dataset_id == run.dataset_id).delete()
                session.commit()
                print(run)


with DAG(
    "setup-environment",
    default_args={
        "email": [CONFIG.email],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "cwd": CONFIG.local_prefix,
        "start_date": CONFIG.start_date,
    },
    description="Tasks related to the cleaning of the environment before collection templates are created.",
    start_date=CONFIG.start_date,
    catchup=False,
    tags=["prepare"],
    schedule=CONFIG.run_interval,
) as dag:
    remove_files_task = PythonOperator(
        task_id="remove-files",
        python_callable=remove_intermediate_files
    )

    remove_files_task.doc_md = dedent(
        """\
    #### Task Documentation

    Remove all temporary files in the airflow data directory.
    """
    )

    create_date_file_task = BashOperator(
        outlets=[PROCESS_START_DATE],
        task_id="create-date-file",
        bash_command=f"echo '{CONFIG.date_str}' > {PROCESS_START_DATE.local_name()}"
    )
    create_date_file_task.doc_md = dedent(
        """\
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

    create_buckup_directory_task.doc_md = dedent(
        """\
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

    clear_dags_task.doc_md = dedent(
        """\
    #### Task Documentation

    Clear status of all DAGs related to collection templates.
    """
    )

    remove_files_task >> create_date_file_task >> create_buckup_directory_task >> clear_dags_task

