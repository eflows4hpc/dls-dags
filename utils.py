import json
import os
import stat
import tempfile
from urllib.parse import urlparse

import requests
from airflow.models.connection import Connection
from datacat_integration.hooks import DataCatalogHook
from webdav3.client import Client


def ssh2local_copy(ssh_hook, source: str, target: str):
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()
        lst = sftp_client.listdir(path=source)

        print(f"{len(lst)} objects in {source}")
        mappings = dict()
        for fname in lst:
            local = tempfile.mktemp(prefix="dls", dir=target)
            full_name = os.path.join(source, fname)
            sts = sftp_client.stat(full_name)
            if str(sts).startswith("d"):
                print(f"{full_name} is a directory. Skipping")
                continue

            print(f"Copying {full_name} --> {local}")
            ssh_download(sftp_client=sftp_client, remote=full_name, local=local)
            mappings[local] = fname

    return mappings


def copy_streams(inp, outp, chunk_size=1024 * 1000):
    while True:
        chunk = inp.read(chunk_size)
        if not chunk:
            break
        content_to_write = memoryview(chunk)
        outp.write(content_to_write)


def ssh_download(sftp_client, remote, local):
    # sftp_client.get(remote, local)
    with sftp_client.open(remote, "rb") as i:
        with open(local, "wb") as o:
            i.set_pipelined(pipelined=True)
            copy_streams(inp=i, outp=o)


def file_exist(sftp, name):
    try:
        r = sftp.stat(name)
        return r.st_size
    except:
        return -1


def http2ssh(url: str, ssh_client, remote_name: str, force=True, auth=None):
    sftp_client = ssh_client.open_sftp()
    size = file_exist(sftp=sftp_client, name=remote_name)
    if size > 0:
        print(f"File {remote_name} exists and has {size} bytes")
        if force is not True:
            return 0
        print("Forcing overwrite")

    dirname = os.path.dirname(remote_name)
    ssh_client.exec_command(command=f"mkdir -p {dirname}")
    ssh_client.exec_command(command=f"touch {remote_name}")

    with requests.get(url, stream=True, verify=False, auth=auth, timeout=(3, 900)) as r:
        written = 0
        length = int(r.headers.get('Content-Length', 0))
        with sftp_client.open(remote_name, "wb") as f:
            f.set_pipelined(pipelined=True)
            for chunk in r.iter_content(chunk_size=1024 * 1000):
                written += len(chunk)
                content_to_write = memoryview(chunk)
                f.write(content_to_write)

        print(f"Written {written} bytes. Content-lenght {length}")
        if length>0 and written!=length:
            print('Size mismatch')
            r.raise_for_status()

        return 0


def get_webdav_client(webdav_connid):
    connection = Connection.get_connection_from_secrets(webdav_connid)
    options = {
        "webdav_hostname": f"https://{connection.host}{connection.schema}",
        "webdav_login": connection.login,
        "webdav_password": connection.get_password(),
    }
    return Client(options)


def get_webdav_prefix(client, dirname):
    # not so efficient
    flist = client.list(dirname, get_info=True)
    if not flist:
        print(f"Empty directory {dirname}")
        return None

    got = [fname for fname in flist if fname["path"].endswith(dirname)]
    if not got:
        print("Could not determine the prefix... quiting")
        return None

    prefix = got[0]["path"][0 : -len(dirname)]
    print(f"Determined common prefix: {prefix}")

    return prefix


def walk_dir(client, path, prefix):
    for p in client.list(path, get_info=True):
        curr_name = p["path"]
        if curr_name.startswith(prefix):
            curr_name = curr_name[len(prefix) :]

        if curr_name == path:
            continue

        # will skip empty directories but we can live with that?
        if p["isdir"]:
            yield from walk_dir(client, curr_name, prefix)
            continue
        yield curr_name


class LFSC(object):
    def list(self, path, get_info=True):
        lst = [os.path.realpath(os.path.join(path, el)) for el in os.listdir(path)]
        if not get_info:
            return lst
        return [{"path": el, "isdir": os.path.isdir(el)} for el in lst]


class RFSC(object):
    def __init__(self, client, **kwargs):
        self.client = client

    def list(self, path, get_info=True):
        if not get_info:
            return [el.filename for el in self.client.listdir_attr(path)]
        return [
            {"path": os.path.join(path, el.filename), "isdir": stat.S_ISDIR(el.st_mode)}
            for el in self.client.listdir_attr(path)
        ]


def resolve_oid(oid):
    hook = DataCatalogHook()
    try:
        entry = json.loads(hook.get_entry("dataset", oid))
        webdav_connid = urlparse(entry["url"]).netloc
        print("Will be using webdav connection", webdav_connid)
        dirname = entry["metadata"]["path"]
        print(f"Processing webdav dir: {dirname}")
        return webdav_connid, dirname
    except Exception as e:
        print(f"No entry {oid} in data cat found. Or entry invalid. {e}")
        return -1, -1
