import os
import io
import re
import json
import urllib3
import scrapy
import logging
from time import sleep
import boto3, botocore
from hashlib import sha256
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse, urljoin, quote_plus

# AWS S3 Configuration
DOCUMENTATION_BUCKET = "palmetto-docs"
POLICY_BUCKET = "ccit-docs"
GITHUB_S3_FOLDER = "github-md-files"
WEBSITE_S3_FOLDER = "website-html-files"
BOOK_S3_FOLDER = "book-pdf-files"

AURORA_SLEEP_WAIT_TIME = 5

# Dictionary of Knowledge Bases to sync
# Format: Knowledge Base ID: [Data Sources to Sync]
KNOWLEDGE_BASES = {
    'OACW6QTO3Q': ['5ESCMTRS6X'],
    'QTOUTNGPGG': ['NJIKFBA9AH']
}

# Paths for Scraper to Ignore
# Format: Site: [Paths]
IGNORED_PATHS = {
    "slurm.schedmd.com": ["/archive"],
}

# Content Types the Scraper will Accept
ACCEPTED_CONTENT_TYPES = [
    'text/html',
    'text/plain',
    'text/markdown',
    'text/csv',
    'text/pdf'
]

# Valid File Extensions
ACCEPTED_FILE_EXTENSIONS = [
    '.html',
    '.txt',
    '.md',
    '.csv',
    '.pdf'
]

DOCUMENTATION_REPOS = [
    "https://github.com/clemsonciti/palmetto-examples"
]

DOCUMENTATION_SITES = [
    "https://docs.rcd.clemson.edu/",
    "https://slurm.schedmd.com/",
    "https://docs.globus.org/globus-connect-personal/",
    "https://docs.globus.org/guides/tutorials/",
    "https://clemsonciti.github.io/rcde_workshops/"
]

POLICY_SITES = [
    "https://ccit.clemson.edu/cybersecurity/policy/",
]

POLICY_BOOK_LIST = "https://clemsonpub.cfmnetwork.com/PublicPageViewList.aspx?id=16"
POLICY_BOOK_DOWNLOAD = "https://clemsonpub.cfmnetwork.com/BookPrint.aspx"

# Initial setup configuration
http = urllib3.PoolManager()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def upload_to_s3(file_key: str, s3_bucket: str, data):
    """
    Uploads data to an s3 bucket with a specified key.
    """

    local_hash = sha256(data).hexdigest()

    try:
        response = s3_client.head_object(Bucket=s3_bucket, Key=file_key)

        remote_hash = response['Metadata'].get('sha256', '')

        if local_hash == remote_hash:
            logger.info(f"Skipped upload because {file_key} unchanged")
            return
        else:
            logger.info(f"Uploading {file_key} because it has changed")

    except Exception as e:
        if isinstance(e, botocore.exceptions.ClientError) and e.response['Error']['Code'] == '404':
            logger.info(f"Uploading {file_key} because it does not exist")
        else:
            raise

    s3_client.upload_fileobj(
            Fileobj=io.BytesIO(data),
            Bucket=s3_bucket,
            Key=file_key,
            ExtraArgs={'Metadata': {'sha256': local_hash}}
    )

    logger.info(f"Uploaded {file_key} with hash {local_hash} to {s3_bucket}")


def get_github_files(repo_url, file_type):
    """
    Fetches files of a specific type from a public GitHub repository.
    """

    repo_api_url = repo_url.replace("github.com", "api.github.com/repos")

    repo_response = http.request('GET', repo_api_url)
    if repo_response.status != 200:
        logger.info(f"Error fetching repository metadata: HTTP {repo_response.status}")
        return []

    repo_data = json.loads(repo_response.data.decode('utf-8'))
    default_branch = repo_data.get("default_branch", "main")

    api_url = f"{repo_api_url}/git/trees/{default_branch}?recursive=1"
    response = http.request('GET', api_url)

    if response.status != 200:
        logger.error(f"Error fetching repository data: HTTP {response.status}")
        return []

    data = json.loads(response.data.decode('utf-8'))
    files = [file for file in data.get('tree', []) if file.get('type') == 'blob' and file.get('path', '').endswith(f"{file_type}")]

    return files, default_branch

def download_and_upload_github(repo_url_list):
    """
    Downloads acceptable file types from GitHub and uploads them to S3.
    """

    logger.info(f"\nDownloading files from GitHub and uploading to S3...\n")

    for repo_url in repo_url_list:
        for file_type in ACCEPTED_FILE_EXTENSIONS:
            files, default_branch = get_github_files(repo_url, file_type)

            for file in files:
                file_url = f"{repo_url}/raw/{default_branch}/{file['path']}"
                response = http.request('GET', file_url)

                if response.status != 200:
                    logger.error(f"Failed to fetch file: {file['path']} (HTTP {response.status})")
                    continue

                parsed_repo = urlparse(repo_url)

                repo_name = parsed_repo.path.strip("/").split("/")[-1]
                file_key = os.path.join(GITHUB_S3_FOLDER, repo_name, file['path'])
                upload_to_s3(file_key, DOCUMENTATION_BUCKET, response.data)

def download_and_upload_books():
    """
    Downloads book PDFs and uploads them to S3.
    """

    response = http.request('GET', POLICY_BOOK_LIST)
    decoded_response = response.data.decode('utf-8', errors='ignore')
    book_list = re.findall(r'BookId=(\d+)', decoded_response)

    for book in book_list:
        book_url = f"{POLICY_BOOK_DOWNLOAD}?IsPDF=1&BookId={book}"
        response = http.request('GET', book_url)

        if response.status != 200:
            print(f"Failed to fetch book {book} (HTTP {response.status})")
            continue

        file_key = os.path.join(BOOK_S3_FOLDER, f"{book}.pdf")
        upload_to_s3(file_key, POLICY_BUCKET, response.data)


class WebsiteSpider(scrapy.Spider):
    name = "website_spider"
    allowed_domains = []
    start_urls = []

    def __init__(self, websites=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if websites:
            self.start_urls = websites
            self.allowed_domains = [urlparse(url).netloc for url in websites]

    def parse(self, response):
        content_type = response.headers.get('Content-Type', b'').decode('utf-8')

        # Ignoring invalid media
        if not any(valid_type in content_type for valid_type in ACCEPTED_CONTENT_TYPES):
            return

        parsed_url = urlparse(response.url)

        url_path = os.path.dirname(parsed_url.path)
        url_endpoint = os.path.basename(parsed_url.path) or "index"

        query_suffix = "_" + quote_plus(parsed_url.query) if parsed_url.query else ""

        filename = (
            # Use endpoint as filename if it has appropriate extension
            url_endpoint if url_endpoint.endswith(tuple(ACCEPTED_FILE_EXTENSIONS))
            # Otherwise use the endpoint and any applied query suffix
            else f"{url_endpoint + query_suffix}.html"
        )

        file_key = os.path.join(
            WEBSITE_S3_FOLDER,
            parsed_url.netloc + url_path,
            filename
        )

        s3_bucket = DOCUMENTATION_BUCKET if any(parsed_url.netloc in site for site in DOCUMENTATION_SITES) else POLICY_BUCKET

        # Upload new or changed content
        upload_to_s3(file_key, s3_bucket, response.body)

        for link in response.css("a::attr(href)").getall():
            # Skipping recursive and invalid links
            if link.startswith("#") or link.startswith("mailto:"):
                continue

            dest_url = response.urljoin(link)
            parsed_dest_url = urlparse(dest_url)

            # Prevents from crossing into separate subdomains
            if parsed_dest_url.netloc not in self.allowed_domains:
                logger.info(f"Skipping subdomain or external link: {dest_url}")
                continue

            # Prevents from traversing any ignored paths
            for ignore in IGNORED_PATHS.get(parsed_dest_url.netloc, []):
                if parsed_dest_url.path.startswith(ignore):
                    self.logger.info(f"Skipping link to {dest_url}")
                    break
            else:
                yield response.follow(link, callback=self.parse)

def run_scraper(websites):
    """
    Starts the web scraper on the supplied website list.
    """

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'LOG_LEVEL': 'INFO',
        'DEPTH_LIMIT': 5,
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
    })
    process.crawl(WebsiteSpider, websites=websites)
    process.start()

def sync_knowledgebases():
    """
    Syncs content knowledge bases.
    """
    
    client = boto3.client('bedrock-agent')
    logger.info("Syncing Knowledge Bases...")

    incomplete = [(key, value) for key, values in KNOWLEDGE_BASES.items() for value in values]

    # While there are still items to process, keep processing
    while incomplete:
        for kb, ds in incomplete:
            try:       
                response = client.start_ingestion_job(
                    knowledgeBaseId=kb,
                    dataSourceId=ds
                )
                
                incomplete.remove((kb, ds))

                logger.info(f"Data Store {ds} in Knowledge Base {kb} synced!")

            except Exception as e:
                # Handles ValidationException raised when Aurora DB is paused
                if isinstance(e, botocore.exceptions.ClientError) and e.response['Error']['Code'] == 'ValidationException':
                    logger.error(f"Aurora DB auto-paused. Sleeping for {AURORA_SLEEP_WAIT_TIME} seconds to allow it to auto resume.")
                    sleep(AURORA_SLEEP_WAIT_TIME)
                
                # Handles ConflictException raised when Knowledge Base is already syncing
                elif isinstance(e, botocore.exceptions.ClientError) and e.response['Error']['Code'] == "ConflictException":
                    logger.error(f"Knowledge Base Sync already in progress. Sleeping for {AURORA_SLEEP_WAIT_TIME} seconds to allow it to complete.")
                    sleep(AURORA_SLEEP_WAIT_TIME)
                    
                    incomplete.remove((kb, ds))

                    logger.info(f"Data Store {ds} in Knowledge Base {kb} synced!")
                else:
                    logger.error(f"Knowledge Base Sync failed to execute: {str(e)}")
                    raise e

def lambda_handler(event, context):

    download_and_upload_github(DOCUMENTATION_REPOS)
    run_scraper(DOCUMENTATION_SITES + POLICY_SITES)
    download_and_upload_books()
    sync_knowledgebases()

    response = {
        "statusCode": 200,
        "body": ""
    }

    logger.info("Response: %s", response)

    return response
