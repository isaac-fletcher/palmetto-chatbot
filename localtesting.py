import os
import urllib3
import json
import requests


# Set the new local save directory
BASE_DIRECTORY = "Desktop/Users/levileard/Desktop/CLEMSON/Computer Science/CPSC_4910/dataingestiontesting"


# Define subfolders for organization
LOCAL_GITHUB_FOLDER = os.path.join(BASE_DIRECTORY, "github-md-files")
LOCAL_WEBSITE_FOLDER = os.path.join(BASE_DIRECTORY, "website-html-files")


http = urllib3.PoolManager()


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
            file_response = http.request('GET', file_url)


            if file_response.status != 200:
                print(f"Failed to fetch file: {file['path']} (HTTP {file_response.status})")
                continue


            with open(file_path, 'wb') as f:
                f.write(file_response.data)
           
            print(f"Saved: {file_path}")


        except Exception as e:
            print(f"Error downloading {file['path']}: {e}")


    print(f"Successfully downloaded {len(files)} {file_type} files to {LOCAL_GITHUB_FOLDER}")


def download_website_html(websites):
    """
    Scrapes .html content from websites and saves them locally
    """
    print(f"\nDownloading HTML files from websites...\n")
   
    for site_url in websites:
        try:
            response = requests.get(site_url)
            if response.status_code != 200:
                print(f"Failed to access {site_url}, status: {response.status_code}")
                continue


            # Extract filename from URL
            filename = site_url.rstrip("/").split("/")[-1] or "index"
            file_path = os.path.join(LOCAL_WEBSITE_FOLDER, f"{filename}.html")


            with open(file_path, 'wb') as f:
                f.write(response.content)


            print(f"Saved: {file_path}")


        except Exception as e:
            print(f"Error downloading {site_url}: {e}")


    print(f"Successfully downloaded HTML files to {LOCAL_WEBSITE_FOLDER}")


def main():
    create_folders()  # Ensure directories exist


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


    # Download from GitHub
    download_github_files(github_repo_url, "md")


    # Download from websites
    download_website_html(websites)


if __name__ == "__main__":
    main()


