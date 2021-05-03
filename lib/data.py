import requests
import zipfile
import pathlib
import re
import os
from urllib.parse import urlparse, unquote

_current_dir = os.path.dirname(os.path.realpath(__file__))


def download(url):
    data_dir = os.path.join(_current_dir, "../data")
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    o = urlparse(url)
    filename = unquote(o.path).split("/")[-1].lower()
    filename = re.sub(r'\s+', '_', filename)
    full_filename = os.path.join(data_dir, filename)
    if os.path.isfile(full_filename):
        return full_filename

    resp = requests.get(url, allow_redirects=True)
    if resp.status_code >= 400:
        raise Exception("Error getting %s, status code = %d" %
                        (filename, resp.status_code))
    with open(full_filename, "wb") as f:
        f.write(resp.content)
    return full_filename


def extract_zip(path):
    folder_name = os.path.basename(path)
    if folder_name.endswith('.zip'):
        folder_name = folder_name[:-4]
    folder_path = os.path.join(_current_dir, "../data", folder_name)
    if os.path.isdir(folder_path):
        return folder_path
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(folder_path)
    return folder_path
