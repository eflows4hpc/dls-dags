from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.models.connection import Connection
from airflow.models import Variable
from airflow.operators.python import get_current_context

from datacat_integration.hooks import DataCatalogHook
from datacat_integration.connection import DataCatalogEntry

from b2shareoperator import (download_file, get_file_list, get_object_md,
                             get_record_template, create_draft_record, add_file, submit_draft)
from decors import get_connection
import docker_cmd as doc
from docker_cmd import WORKER_DATA_LOCATION
import os

"""This piplines is a test case for starting a clusterting algorithm with HeAT, running in a Docker environment.
A test set of parameters with a HeAT example:
{"oid": "b143bf73efd24d149bba4c081964b459", "image": "ghcr.io/helmholtz-analytics/heat:1.1.1-alpha", "stagein_args": ["demo_knn.py", "iris.h5"], "stageout_args": ["result.out"], "entrypoint": "/bin/bash", "command": "python"}
Params:
    oid (str): oid of the data
    image (str): a docker contianer image
    stagein_args (list): a list of stage in files necesarry for the executeion
    stageout_args (list): a list of stage out files which are results from the execution
    string_args (str): a string of further arguments which might be needed for the task execution
    entrypoint (str): you can specify or overwrite the docker entrypoint
    command (str): you can specify or override the command to be executed
    args_to_dockerrun (str): docker run additional options
"""

default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example', 'docker'])
def docker_in_worker():
    DW_CONNECTION_ID = "docker_worker"


    @task()
    def stagein(**kwargs):
        """ stage in task
        This task gets the 'datacat_oid' or 'oid' from the DAG params to retreive a connection from it (b2share for now).
        It then downloads all data from the b2share entry to the local disk, and returns a mapping of these files to the local download location,
        which can be used by the following tasks.
        """
        params = kwargs['params']
        datacat_hook = DataCatalogHook()
        
        if 'oid' not in params:  # {"oid": "b143bf73efd24d149bba4c081964b459"}
            if 'datacat_oid' not in params:
                print("Missing object id in pipeline parameters")
                return -1  # non zero exit code is a task failure
            else:
                params['oid'] = params['datacat_oid']
        oid_split = params['oid'].split("/")
        type = 'dataset'
        oid = 'placeholder_text'
        if len(oid_split) is 2:
            type = oid_split[0]
            oid = oid_split[1]
        elif len(oid_split) is 1:
            oid = oid_split[0]
        else:
            print("Malformed oid passed as parameter.")
            return -1

        entry = DataCatalogEntry.from_json(datacat_hook.get_entry(type, oid))

        print(f"using entry: {entry}")
        b2share_server_uri = entry.url
        # TODO general stage in based on type metadata
        # using only b2share for now
        b2share_oid = entry.metadata['b2share_oid']

        obj = get_object_md(server=b2share_server_uri, oid=b2share_oid)
        print(f"Retrieved object {oid}: {obj}")
        flist = get_file_list(obj)
        
        name_mappings = {}
        tmp_dir = Variable.get("working_dir", default_var='/tmp/')
        print(f"Local working dir is: {tmp_dir}")
        
        for fname, url in flist.items():
            print(f"Processing: {fname} --> {url}")
            tmpname = download_file(url=url, target_dir=tmp_dir)
            name_mappings[fname] = tmpname
            
        return name_mappings   
   
    @task()
    def load(files: dict, **kwargs):
        """This task copies the data to a location, 
        which will enable the following tasks an access to the data

        Args:
            files (dict): the files that will be stored on another system
        Returns:
            list: the locations of the newly loaded files
        """
        print(f"Using {DW_CONNECTION_ID} connection")
        ssh_hook = get_connection(conn_id=DW_CONNECTION_ID)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            for [truename, local] in files.items():
                print(
                    f"Copying {local} --> {DW_CONNECTION_ID}:{os.path.join(WORKER_DATA_LOCATION, truename)}")
                sftp_client.put(local, os.path.join(WORKER_DATA_LOCATION, truename))
                # or separate cleanup task?
                os.unlink(local)

        # loaded_files = []
        # for [truename, local_path] in files.items():
            
        #     destination = shutil.copy(local_path, os.path.join(DATA_LOCATION, truename))
        #     print(f"Copying {local_path} --> copying to: {destination};")
        #     loaded_files.append(destination)
        # os.unlink(local_path)

        # return loaded_files

    @task
    def run_container(data_locations, **kwargs):
        
        params = kwargs['params']
        stageout_fnames = params.get('stageout_args', []) 
        
        cmd = doc.get_dockercmd(params, WORKER_DATA_LOCATION)
        print(f"Executing docker command {cmd}")
        
        print(f"Using {DW_CONNECTION_ID} connection")
        hook = get_connection(conn_id=DW_CONNECTION_ID)
        
        task_calculate = SSHOperator(
            task_id="calculate",
            ssh_hook=hook,
            command=cmd
        )
        
        context = get_current_context()
        task_calculate.execute(context)
        
        return stageout_fnames

    @task
    def ls_results(output_files: list):
        if not output_files:
            return "No output to stage out. Nothing more to do."
        hook = get_connection(conn_id=DW_CONNECTION_ID)
        sp = " "
        cmd = f"cd {WORKER_DATA_LOCATION}; ls -al {sp.join(output_files)}"
        process = SSHOperator(
            task_id="print_results",
            ssh_hook=hook,
            command=cmd
        )
        context = get_current_context()
        process.execute(context)    
    
    @task()
    def retrieve_res(fnames: list, **kwargs):
        """This task copies the data from the remote docker worker back to airflow workspace

        Args:
            fnames (list): the files to be retrieved from the docker worker 
        Returns:
            local_fpath (list): the path of the files copied back to the airflow host
        """
        local_tmp_dir = Variable.get("working_dir", default_var='/tmp/')
        local_fpath = []
        print(f"Using {DW_CONNECTION_ID} connection")
        ssh_hook = get_connection(conn_id=DW_CONNECTION_ID)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            for name in fnames:
                l = os.path.join(local_tmp_dir, name)
                print(f"Copying {os.path.join(WORKER_DATA_LOCATION, name)} to {l}")
                sftp_client.get(os.path.join(WORKER_DATA_LOCATION, name), l)
                local_fpath.append(l)
        
        return local_fpath
    
    @task()
    def cleanup_doc_worker(files, **kwargs):
        """This task deletes all the files from the docker worker

        # Args:
        #     fnames (list): the result files to be deleted on the docker worker  
        """
        params = kwargs['params']
        stagein_fnames = params.get('stagein_args', [])
        stageout_fnames = params.get('stageout_args', []) 
        all_fnames = stagein_fnames + stageout_fnames
        print(f"Using {DW_CONNECTION_ID} connection")
        ssh_hook = get_connection(conn_id=DW_CONNECTION_ID)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            for file in all_fnames:
                print(
                    f"Deleting file {DW_CONNECTION_ID}:{os.path.join(WORKER_DATA_LOCATION, file)}")
                sftp_client.remove(os.path.join(WORKER_DATA_LOCATION, file))
        
                
    @task
    def stageout_results(output_files: list):
        if not output_files:
            print("No output to stage out. Nothing more to do.")
            return -1
        connection = Connection.get_connection_from_secrets('default_b2share')
        
        server = "https://" + connection.host
        token = ''
        if 'access_token' in connection.extra_dejson.keys():
            token = connection.extra_dejson['access_token']
        print(f"Registering data to {server}")
        template = get_record_template()
        
        r = create_draft_record(server=server, token=token, record=template)
        print(f"record {r}")
        if 'id' in r:
            print(f"Draft record created {r['id']} --> {r['links']['self']}")
        else:
            print('Something went wrong with registration', r, r.text)
            return -1
        
        for f in output_files:
            print(f"Uploading {f}")
            _ = add_file(record=r, fname=f, token=token, remote=f)
            # delete local
            # os.unlink(local)
        
        print("Submitting record for pubication")
        submitted = submit_draft(record=r, token=token)
        print(f"Record created {submitted}")

        return submitted['links']['publication']
        # context = get_current_context()
        # process.execute(context)    
        
    #TODO a cleanup job
    @task
    def cleanup_local(errcode, res_fpaths):
        if type(errcode) == int:
            print("The data could not be staged out in the repository. Cleaning up")

        for f in res_fpaths:
            print(f"Deleting file: {f}")
            os.remove(f)
            #delete local copies of file
    
    @task()
    def register(object_url, additional_metadata = {}, **kwargs):
        reg = kwargs['params']['register']
        if not reg or reg is not "True":
            print("Skipping registration as 'register' parameter is not set")
            return 0

        hook = DataCatalogHook()
        print("Connected to datacat via hook")

        if not additional_metadata['author']:
            additional_metadata['author'] = "DLS on behalft of eFlows"
        
        if not additional_metadata['access']:
            additional_metadata['access'] = "hook-based"
    
        entry = DataCatalogEntry(name=f"DLS results {kwargs['run_id']}",
                                 url=object_url,
                                 metadata=additional_metadata
                                )
        try:
            r = hook.create_entry(datacat_type='dataset', entry=entry)
            print("Hook registration returned: ", r)
            return f"{hook.base_url}/dataset/{r}" 
        except ConnectionError as e:
            print('Registration failed', e)
            return -1
            

    files = stagein()
    data_locations = load(files)
    output_fnames = run_container(data_locations)
    ls_results(output_fnames)
    res_fpaths = retrieve_res(output_fnames)
    cleanup_doc_worker(res_fpaths)
    url_or_errcode = stageout_results(res_fpaths)
    cleanup_local(url_or_errcode, res_fpaths)
    register(url_or_errcode)

    # files >> data_locations >> output_fnames >> ls_results(output_fnames) >> files >> stageout_results(files) >> cleanup()
    
dag = docker_in_worker()

