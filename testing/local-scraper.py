import os
import requests
import json
import scrapy
from scrapy.crawler import CrawlerProcess

# Set the new local save directory
BASE_DIRECTORY = "Desktop/Users/levileard/Desktop/CLEMSON/Computer Science/CPSC_4910/dataingestiontesting"

# Define subfolders for organization
LOCAL_GITHUB_FOLDER = os.path.join(BASE_DIRECTORY, "github-md-files")
LOCAL_WEBSITE_FOLDER = os.path.join(BASE_DIRECTORY, "website-html-files")

def create_folders():
    """Ensure the directories exist before saving files."""
    os.makedirs(LOCAL_GITHUB_FOLDER, exist_ok=True)
    os.makedirs(LOCAL_WEBSITE_FOLDER, exist_ok=True)

def get_github_files(repo_url, file_type):
    """
    Fetches files of a specific type from a public GitHub repository.
    """
    repo_api_url = repo_url.replace("github.com", "api.github.com/repos")

    # Get repo metadata
    repo_response = requests.get(repo_api_url)
    if repo_response.status_code != 200:
        print(f"Error fetching repository metadata: HTTP {repo_response.status_code}")
        return []

    repo_data = repo_response.json()
    default_branch = repo_data.get("default_branch", "main")

    # Get repo files
    api_url = f"{repo_api_url}/git/trees/{default_branch}?recursive=1"
    response = requests.get(api_url)

    if response.status_code != 200:
        print(f"Error fetching repository data: HTTP {response.status_code}")
        return []

    data = response.json()
    files = [file for file in data.get('tree', []) if file.get('type') == 'blob' and file.get('path', '').endswith(f".{file_type}")]

    return files, default_branch

def download_github_files(repo_url, file_type):
    """
    Downloads .md files from GitHub and saves them locally
    """
    print(f"\nDownloading {file_type} files from GitHub...\n")
    files, default_branch = get_github_files(repo_url, file_type)

    for file in files:
        file_url = f"{repo_url}/raw/{default_branch}/{file['path']}"
        file_path = os.path.join(LOCAL_GITHUB_FOLDER, file['path'].replace("/", "_"))  # Flatten folder structure

        try:
            file_response = requests.get(file_url)

            if file_response.status_code != 200:
                print(f"Failed to fetch file: {file['path']} (HTTP {file_response.status_code})")
                continue

            with open(file_path, 'wb') as f:
                f.write(file_response.content)

            print(f"Saved: {file_path}")

        except Exception as e:
            print(f"Error downloading {file['path']}: {e}")

    print(f"Successfully downloaded {len(files)} {file_type} files to {LOCAL_GITHUB_FOLDER}")

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
        folder_path = os.path.join(LOCAL_WEBSITE_FOLDER, url_path[:url_path.rfind("/")])
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"{filename}.html")

        with open(file_path, 'wb') as f:
            f.write(response.body)

        self.log(f"Saved: {file_path}")

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

def main():
    create_folders()  # Ensure directories exist

    github_repo_url = "https://github.com/clemsonciti/palmetto-examples"

    websites = [
        "https://docs.rcd.clemson.edu/",
        "https://slurm.schedmd.com/documentation.html",
        "https://docs.globus.org/globus-connect-personal/",
        "https://docs.globus.org/guides/tutorials/manage-files/transfer-files/",
        "https://docs.globus.org/guides/tutorials/manage-files/share-files/"
    ]

    download_github_files(github_repo_url, "md")
    run_scraper(websites)

if __name__ == "__main__":
    main()
