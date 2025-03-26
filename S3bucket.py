import os
import io
import json
import urllib3
import boto3
import scrapy
import logging
from scrapy.crawler import CrawlerProcess

# AWS S3 Configuration
S3_BUCKET_NAME = "palmetto-docs"              # Name of the S3 bucket to upload files
GITHUB_S3_FOLDER = "github-md-files"          # Folder in S3 to store GitHub Markdown files
WEBSITE_S3_FOLDER = "website-html-files"      # Folder in S3 to store scraped HTML pages

# HTTP connection pool manager for efficient HTTP requests
http = urllib3.PoolManager()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_github_files(repo_url, file_type):
    """
    Fetches all files of a specific type (e.g., 'md') from a public GitHub repository.
    Returns a list of file metadata and the repository's default branch.
    """
    repo_api_url = repo_url.replace("github.com", "api.github.com/repos")

    # Get repository metadata (to retrieve the default branch)
    repo_response = http.request('GET', repo_api_url)
    if repo_response.status != 200:
        print(f"Error fetching repository metadata: HTTP {repo_response.status}")
        return []

    repo_data = json.loads(repo_response.data.decode('utf-8'))
    default_branch = repo_data.get("default_branch", "main")

    # Get a list of all files in the repository recursively
    api_url = f"{repo_api_url}/git/trees/{default_branch}?recursive=1"
    response = http.request('GET', api_url)

    if response.status != 200:
        print(f"Error fetching repository data: HTTP {response.status}")
        return []

    data = json.loads(response.data.decode('utf-8'))

    # Filter for files that match the specified file type (e.g., '.md')
    files = [
        file for file in data.get('tree', [])
        if file.get('type') == 'blob' and file.get('path', '').endswith(f".{file_type}")
    ]
    
    return files, default_branch

def download_and_upload_github(repo_url, file_type, bucket_name, s3_folder):
    """
    Downloads files (like Markdown) from GitHub and uploads them to the specified S3 folder.
    """
    print(f"\nDownloading {file_type} files from GitHub and uploading to S3...\n")
    files, default_branch = get_github_files(repo_url, file_type)

    s3_client = boto3.client('s3')

    for file in files:
        # Construct raw GitHub URL to download file content
        file_url = f"{repo_url}/raw/{default_branch}/{file['path']}"
        file_response = http.request('GET', file_url)

        if file_response.status != 200:
            print(f"Failed to fetch file: {file['path']} (HTTP {file_response.status})")
            continue

        file_content = file_response.data
        # Create a flat S3 key to avoid folder nesting
        s3_key = os.path.join(s3_folder, file['path'].replace("/", "_"))

        # Upload to S3
        s3_client.upload_fileobj(io.BytesIO(file_content), bucket_name, s3_key)
        print(f"Uploaded: {s3_key}")

    print(f"Successfully uploaded {len(files)} {file_type} files to S3")

class WebsiteSpider(scrapy.Spider):
    """
    Scrapy Spider to crawl and download HTML content from websites.
    Uploads each HTML page to an S3 bucket.
    """
    name = "website_spider"
    allowed_domains = []
    start_urls = []

    def __init__(self, websites=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if websites:
            self.start_urls = websites
            # Extract domain names for allowed_domains to keep crawl within scope
            self.allowed_domains = [url.split('//')[-1].split('/')[0] for url in websites]

    def parse(self, response):
        """
        Called for each page visited. Uploads page HTML to S3 and follows internal links.
        """
        # Determine filename from URL
        filename = response.url.rstrip("/").split("/")[-1] or "index"
        file_key = os.path.join(WEBSITE_S3_FOLDER, f"{filename}.html")

        # Upload HTML page content to S3
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(io.BytesIO(response.body), S3_BUCKET_NAME, file_key)
        print(f"Uploaded {response.url} to S3 as {file_key}")

        # Follow and crawl additional links within the same domain
        for link in response.css("a::attr(href)").getall():
            if link.startswith("/") or any(domain in link for domain in self.allowed_domains):
                yield response.follow(link, callback=self.parse)

def run_scraper(websites):
    """
    Initializes and runs the Scrapy crawler on the provided list of websites.
    """
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'LOG_LEVEL': 'INFO'
    })
    process.crawl(WebsiteSpider, websites=websites)
    process.start()

def lambda_handler(event, context):
    """
    AWS Lambda handler function. Downloads GitHub files and scrapes websites,
    uploading all results to the S3 bucket.
    """
    github_repo_url = "https://github.com/clemsonciti/palmetto-examples"

    # List of websites to crawl and upload HTML to S3
    websites = [
        "https://docs.rcd.clemson.edu/",
        "https://slurm.schedmd.com/documentation.html",
        "https://docs.globus.org/globus-connect-personal/",
        "https://docs.globus.org/guides/tutorials/manage-files/transfer-files/",
        "https://docs.globus.org/guides/tutorials/manage-files/share-files/"
    ]

    # Fetch and upload GitHub markdown files
    download_and_upload_github(github_repo_url, "md", S3_BUCKET_NAME, GITHUB_S3_FOLDER)
    
    # Scrape and upload website HTML content
    run_scraper(websites)

    response = {
        "statusCode": 200,
        "body": ""
    }

    logger.info("Response: %s", response)
    return response
