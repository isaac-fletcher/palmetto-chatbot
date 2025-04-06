import os
import io
import json
import urllib3
import boto3
import scrapy
import logging
from scrapy.crawler import CrawlerProcess

# AWS S3 Configuration
DOCUMENTATION_BUCKET = "palmetto-docs"
POLICY_BUCKET = "ccit-docs"
GITHUB_S3_FOLDER = "github-md-files"
WEBSITE_S3_FOLDER = "website-html-files"
HARDWARE_S3_FOLDER = "lambda-responses"

DOCUMENTATION_REPOS = [
    "https://github.com/clemsonciti/palmetto-examples"
]
    
DOCUMENTATION_SITES = [
    "https://docs.rcd.clemson.edu/",
    "https://slurm.schedmd.com/documentation.html",
    "https://docs.globus.org/globus-connect-personal/",
    "https://docs.globus.org/guides/tutorials/manage-files/transfer-files/",
    "https://docs.globus.org/guides/tutorials/manage-files/share-files/",
    "https://clemsonciti.github.io/rcde_workshops/index.html",
    "https://github.com/clemsonciti/palmetto-examples"
]

POLICY_SITES = [
    "https://ccit.clemson.edu/cybersecurity/policy/",
    "https://clemsonpub.cfmnetwork.com/"
]

http = urllib3.PoolManager()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Adding lambda client
lambda_client = boto3.client('lambda')

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
    files = [file for file in data.get('tree', []) if file.get('type') == 'blob' and file.get('path', '').endswith(f".{file_type}")]
    
    return files, default_branch

def download_and_upload_github(repo_url_list, file_type):
    """
    Downloads .md files from GitHub and uploads them to S3.
    """
    logger.info(f"\nDownloading {file_type} files from GitHub and uploading to S3...\n")
    
    for repo_url in repo_url_list:
        files, default_branch = get_github_files(repo_url, file_type)
        
        s3_client = boto3.client('s3')
        for file in files:
            file_url = f"{repo_url}/raw/{default_branch}/{file['path']}"
            file_response = http.request('GET', file_url)

            if file_response.status != 200:
                logger.error(f"Failed to fetch file: {file['path']} (HTTP {file_response.status})")
                continue
            
            file_content = file_response.data
            s3_key = os.path.join(GITHUB_S3_FOLDER, repo_url, file['path'].replace("/", "_"))  # Flatten folder structure
            s3_client.upload_fileobj(io.BytesIO(file_content), DOCUMENTATION_BUCKET, s3_key)
            logger.info(f"Uploaded: {s3_key}")
    
        logger.info(f"Successfully uploaded {len(files)} {file_type} files to S3")

class WebsiteSpider(scrapy.Spider):
    name = "website_spider"
    allowed_domains = []
    start_urls = []

    def __init__(self, websites=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if websites:
            self.start_urls = websites
            self.allowed_domains = [url.split('//')[-1].split('/')[0] for url in websites]

    def parse(self, response):
        url_path = response.url.replace("https://", "").replace("http://", "").rstrip("/")
        filename = url_path.split("/")[-1] or "index"
        file_key = os.path.join(WEBSITE_S3_FOLDER, url_path[:url_path.rfind("/")], f"{filename}.html")
        
        s3_client = boto3.client('s3')
        s3_bucket = DOCUMENTATION_BUCKET if url_path in DOCUMENTATION_SITES else POLICY_BUCKET
        s3_client.upload_fileobj(io.BytesIO(response.body), s3_bucket, file_key)
        logger.info(f"Uploaded {response.url} to S3 as {file_key}")
        
        for link in response.css("a::attr(href)").getall():
            if link.startswith("/") or any(domain in link for domain in self.allowed_domains):
                yield response.follow(link, callback=self.parse)

def run_scraper(websites):
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'LOG_LEVEL': 'INFO'
    })
    process.crawl(WebsiteSpider, websites=websites)
    process.start()


def lambda_handler(event, context):
       
    download_and_upload_github(DOCUMENTATION_REPOS, "md")
    run_scraper(DOCUMENTATION_SITES + POLICY_SITES)

    response = {
        "statusCode": 200,
        "body": ""
    }

    logger.info("Response: %s", response)

    return response

