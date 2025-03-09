import urllib3
import boto3
import os
import io
import json
import requests
from bs4 import BeautifulSoup

# AWS S3 Configuration
S3_BUCKET_NAME = "palmettotestbucket202503"  # Replace with your actual S3 bucket name
GITHUB_S3_FOLDER = "github-md-files"
WEBSITE_S3_FOLDER = "website-html-files"

http = urllib3.PoolManager()

def get_github_files(repo_url, file_type):
    """
    Fetches files of a specific type from a public GitHub repository.
    """
    repo_api_url = repo_url.replace("github.com", "api.github.com/repos")
    
    # Get repo metadata
    repo_response = http.request('GET', repo_api_url)
    if repo_response.status != 200:
        print(f"Error fetching repository metadata: HTTP {repo_response.status}")
        return []
    
    repo_data = json.loads(repo_response.data.decode('utf-8'))
    default_branch = repo_data.get("default_branch", "main")

    # Get repo files
    api_url = f"{repo_api_url}/git/trees/{default_branch}?recursive=1"
    response = http.request('GET', api_url)

    if response.status != 200:
        print(f"Error fetching repository data: HTTP {response.status}")
        return []
    
    data = json.loads(response.data.decode('utf-8'))
    files = [file for file in data.get('tree', []) if file.get('type') == 'blob' and file.get('path', '').endswith(f".{file_type}")]
    
    return files, default_branch

def download_and_upload_github(repo_url, file_type, bucket_name, s3_folder):
    """
    Downloads .md files from GitHub and uploads them to S3.
    """
    files, default_branch = get_github_files(repo_url, file_type)
    
    s3_client = boto3.client('s3')
    for file in files:
        file_url = f"{repo_url}/raw/{default_branch}/{file['path']}"
        file_response = http.request('GET', file_url)

        if file_response.status != 200:
            print(f"Failed to fetch file: {file['path']} (HTTP {file_response.status})")
            continue
        
        file_content = file_response.data
        s3_key = os.path.join(s3_folder, file['path'])
        s3_client.upload_fileobj(io.BytesIO(file_content), bucket_name, s3_key)
    
    print(f"Successfully uploaded {len(files)} {file_type} files to S3")

def download_and_upload_html(websites, bucket_name, s3_folder):
    """
    Scrapes .html content from websites and uploads to S3.
    """
    s3_client = boto3.client('s3')

    for site_url in websites:
        try:
            response = requests.get(site_url)
            if response.status_code != 200:
                print(f"Failed to access {site_url}, status: {response.status_code}")
                continue

            # Extract filename from URL
            filename = site_url.rstrip("/").split("/")[-1] or "index"
            file_key = os.path.join(s3_folder, f"{filename}.html")

            # Upload HTML content
            s3_client.upload_fileobj(io.BytesIO(response.content), bucket_name, file_key)
            print(f"Uploaded {site_url} to S3 as {file_key}")

        except Exception as e:
            print(f"Error downloading {site_url}: {e}")

def main():
    # GitHub repo for .md files
    github_repo_url = "https://github.com/clemsonciti/palmetto-examples"

    # List of websites to scrape .html files from
    websites = [
        "https://docs.rcd.clemson.edu/",
        "https://slurm.schedmd.com/documentation.html",
        "https://docs.globus.org/globus-connect-personal/",
        "https://docs.globus.org/guides/tutorials/manage-files/transfer-files/",
        "https://docs.globus.org/guides/tutorials/manage-files/share-files/"
    ]

    # Download from GitHub and upload to S3
    download_and_upload_github(github_repo_url, "md", S3_BUCKET_NAME, GITHUB_S3_FOLDER)

    # Download from websites and upload to S3
    download_and_upload_html(websites, S3_BUCKET_NAME, WEBSITE_S3_FOLDER)

if __name__ == "__main__":
    main()
