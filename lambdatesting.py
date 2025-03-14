
import json
import re
import os
import sys
import io
import urllib3
import boto3
from urllib.parse import urljoin, urlparse

sys.path.append("/opt/python/lib/python3.13/site-packages")
from playwright.sync_api import sync_playwright


# Environment Variables
S3_BUCKET_NAME = os.environ["palmettobucket2025v0"]  # Set your S3 bucket name
AWS_REGION = os.environ["us-east-1"]  # Set your AWS region, e.g., 'us-east-1'

# AWS Clients
s3_client = boto3.client('s3', region_name=AWS_REGION)

# HTTP Manager

http = urllib3.PoolManager()

def get_github_files(repo_url, file_type):
    """
    Fetches files of a specific type from a public GitHub repository.
    """
    repo_api_url = repo_url.replace("github.com", "api.github.com/repos")

    repo_response = http.request("GET", repo_api_url)
    if repo_response.status != 200:
        print(f"Error fetching repository metadata: HTTP {repo_response.status}")
        return []

    repo_data = json.loads(repo_response.data.decode("utf-8"))
    default_branch = repo_data.get("default_branch", "main")

    api_url = f"{repo_api_url}/git/trees/{default_branch}?recursive=1"
    response = http.request("GET", api_url)

    if response.status != 200:
        print(f"Error fetching repository data: HTTP {response.status}")
        return []

    data = json.loads(response.data.decode("utf-8"))
    files = [
        file
        for file in data.get("tree", [])
        if file.get("type") == "blob" and file.get("path", "").endswith(f".{file_type}")
    ]

    return files, default_branch


def upload_to_s3(file_name, file_content):
    """
    Upload the file content to S3.
    """
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_name, Body=file_content)
    print(f"Uploaded to S3: {file_name}")

def download_and_save_github(repo_url, file_type):
    """
    Downloads .md files from GitHub and uploads them to S3.
    """
    print(f"\nDownloading {file_type} files from GitHub and uploading to S3...\n")
    files, default_branch = get_github_files(repo_url, file_type)

    for file in files:
        file_url = f"{repo_url}/raw/{default_branch}/{file['path']}"
        file_response = http.request("GET", file_url)

        if file_response.status != 200:
            print(f"Failed to fetch file: {file['path']} (HTTP {file_response.status})")
            continue

        file_content = file_response.data
        file_name = file["path"].replace("/", "_")

        upload_to_s3(file_name, file_content)

    print(f"Successfully uploaded {len(files)} {file_type} files to S3.")

visited_urls = set()

def sanitize_filename(url):
    """Sanitize the filename by replacing invalid characters."""
    filename = url.rstrip("/").split("/")[-1] or "index"
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)  # Replace invalid characters
    return filename[:100]  # Limit filename length for safety

def save_html_to_s3(content, filename):
    """Save HTML content to an S3 bucket."""
    upload_to_s3(filename, content)
    print(f"Saved to S3: {filename}")

def get_internal_links(page, base_url):
    """Extract all internal links from a webpage."""
    all_links = page.eval_on_selector_all("a", "elements => elements.map(e => e.href)")
    internal_links = {
        urljoin(base_url, link) for link in all_links
        if link and link.startswith("http") and urlparse(link).netloc == urlparse(base_url).netloc
    }
    return list(internal_links)  # Remove duplicates

def scrape_website(url, depth=3, browser=None):
    """
    Scrapes a website (including JavaScript-rendered content), extracts internal links,
    scrapes those pages as well, and uploads HTML to S3.
    """
    global visited_urls  #Fix: Declare global variable inside function

    if url in visited_urls or depth <= 0:
        return  # Prevent revisiting pages or exceeding depth

    visited_urls.add(url)  # Mark URL as visited

    if browser is None:
        print("Error: Playwright browser instance is not provided!")
        return

    page = browser.new_page()

    try:
        page.goto(url, wait_until="networkidle")  #Ensure JavaScript fully loads
    except Exception as e:
        print(f"Failed to load {url}: {e}")
        return

    # Save main page HTML to S3
    html_content = page.content()
    filename = sanitize_filename(url) + ".html"
    save_html_to_s3(html_content, filename)

    # Extract and process internal links
    links = get_internal_links(page, url)

    for link in links:
        scrape_website(link, depth=depth-1, browser=browser)  # Recursively scrape internal pages

    page.close()


def lambda_handler(event, context):
    """
    Main Lambda handler function to scrape websites and GitHub repo and store content in S3.
    """
    executable_path = '/opt/python/lib/python3.13/site-packages/playwright/driver/chromium/chome'
    websites = event.get("websites", [
        "https://docs.rcd.clemson.edu/",
        "https://slurm.schedmd.com/documentation.html",
        "https://docs.globus.org/globus-connect-personal/",
        "https://docs.globus.org/guides/tutorials/manage-files/transfer-files/",
        "https://docs.globus.org/guides/tutorials/manage-files/share-files/"
    ])
    github_repo_url = event.get("github_repo_url", "https://github.com/clemsonciti/palmetto-examples")  # GitHub repo URL passed in event
    
    if not websites and not github_repo_url:
        return {"statusCode": 400, "body": "No websites or GitHub repository URL provided"}
    
    if github_repo_url:
        file_type = "md"  # Change as needed
        download_and_save_github(github_repo_url, file_type)  # Fetch and upload GitHub files to S3

    with sync_playwright() as p:  # Start Playwright globally
        browser = p.chromium.launch(headless=True)  # Keep browser open for all requests

        for site in websites:
            scrape_website(site, depth=3, browser=browser)  # Pass the same browser instance

        browser.close()  # Close browser after all sites are scraped

    return {"statusCode": 200, "body": "Scraping completed successfully"}

