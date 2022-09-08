from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.models.connection import Connection
from airflow.models import Variable
from airflow.operators.python import get_current_context
from b2shareoperator import (download_file, get_file_list, get_object_md,
                             get_objects, get_record_template, create_draft_record, add_file, submit_draft)
from decors import get_connection
import docker_cmd as doc
from docker_cmd import WORKER_DATA_LOCATION
import os
import uuid

"""This piplines is a test case for starting a clusterting algorithm with HeAT, running in a Docker environment.
A test set of parameters with a HeAT example:
{"oid": "b143bf73efd24d149bba4c081964b459", "image": "ghcr.io/helmholtz-analytics/heat:1.1.1-alpha", "entrypoint": "/bin/bash", "command": "python demo_knn.py iris.h5 result.out"}
{"oid": "b143bf73efd24d149bba4c081964b459", "image":"hello-world"}
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
input_fnames = []

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example', 'docker'])
def docker_in_worker():
    DW_CONNECTION_ID = "docker_worker"
    
    @task(multiple_outputs=True)
    def extract(**kwargs):
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        b2share connection.
        :param oid: ID of the file to be extracted
        """
        connection = Connection.get_connection_from_secrets('default_b2share')
        server = connection.get_uri()
        print(f"Rereiving data from {server}")

        params = kwargs['params']
        if 'oid' not in params:  # {"oid": "b143bf73efd24d149bba4c081964b459"}
            print("Missing object id in pipeline parameters")
            lst = get_objects(server=server)
            flist = {o['id']: [f['key'] for f in o['files']] for o in lst}
            print(f"Objects on server: {flist}")
            return -1  # non zero exit code is a task failure

        oid = params['oid']

        obj = get_object_md(server=server, oid=oid)
        print(f"Retrieved object {oid}: {obj}")
        flist = get_file_list(obj)

        return flist
    
    @task(multiple_outputs=True)
    def transform(flist: dict):
        """
        #### Transform task
        A Transform task which takes in the collection of data, retrieved from the connection, downloads the files 
        and returns a map of the filename with the corresponding filepath.
        """
        name_mappings = {}
        tmp_dir = Variable.get("working_dir", default_var='/tmp/')
        print(f"Local working dir is: {tmp_dir}")
        
        for fname, url in flist.items():
            print(f"Processing: {fname} --> {url}")
            tmpname = download_file(url=url, target_dir=tmp_dir)
            name_mappings[fname] = tmpname
            
        return name_mappings   
   
    @task()
    def move_to_docker_host(files: dict, **kwargs):
        """This task copies the data to a location, 
        which will enable the following tasks an access to the data

        Args:
            files (dict): the files that will be stored on another system
        Returns:
            list: the locations of the newly loaded files
        """
        print(f"Using {DW_CONNECTION_ID} connection")
        ssh_hook = get_connection(conn_id=DW_CONNECTION_ID)
        user_dir_name = str(uuid.uuid4())
        target_dir = os.path.join(WORKER_DATA_LOCATION, user_dir_name)
        
        with ssh_hook.get_conn() as ssh_client:
            
            sftp_client = ssh_client.open_sftp()

            sftp_client.mkdir(target_dir, mode=0o755)
            for [truename, local] in files.items():
                print(
                    f"Copying {local} --> {DW_CONNECTION_ID}:{os.path.join(target_dir, truename)}")
                sftp_client.put(local, os.path.join(target_dir, truename))
                input_fnames.append(truename)
                # or separate cleanup task?
                os.unlink(local)

        # loaded_files = []
        # for [truename, local_path] in files.items():
            
        #     destination = shutil.copy(local_path, os.path.join(DATA_LOCATION, truename))
        #     print(f"Copying {local_path} --> copying to: {destination};")
        #     loaded_files.append(destination)
        # os.unlink(local_path)

        return target_dir

    @task
    def run_container(data_location, **kwargs):
        """A task which runs in the docker worker and spins up a docker container with the an image and giver parameters.

        Args:
            image(str): contianer image
            stageout_args (list): a list of files which are results from the execution
            job_args (str): a string of further arguments which might be needed for the task execution
            entrypoint (str): specify or overwrite the docker entrypoint
            command(str): you can specify or override the command to be executed
            args_to_dockerrun(str): docker options
        """    
        params = kwargs['params']
        # stageout_fnames = params.get('stageout_args', []) 
        
        cmd = doc.get_dockercmd(params, data_location)
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
        
        return data_location

    @task
    def ls_results(output_dir):
        if not output_dir:
            return "No output to stage out. Nothing more to do."
        hook = get_connection(conn_id=DW_CONNECTION_ID)
        
        cmd = f"ls -al {output_dir}"
        process = SSHOperator(
            task_id="print_results",
            ssh_hook=hook,
            command=cmd
        )
        context = get_current_context()
        process.execute(context)    
    
    @task()
    def retrieve_res(output_dir: str, **kwargs):
        """This task copies the data from the remote docker worker back to airflow workspace

        Args:
            output_dir (str): the folder containing all the user files for the executed task, located on the docker worker 
        Returns:
            local_fpath (list): the path of the files copied back to the airflow host
        """
        local_tmp_dir = Variable.get("working_dir", default_var='/tmp/') #str(uuid.uuid4())
        local_fpath = []
        print(f"Using {DW_CONNECTION_ID} connection")
        ssh_hook = get_connection(conn_id=DW_CONNECTION_ID)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            
            for fname in sftp_client.listdir(output_dir):
                if fname not in input_fnames:
                    l = os.path.join(local_tmp_dir, fname)
                    print(f"Copying {os.path.join(output_dir, fname)} to {l}")
                    sftp_client.get(os.path.join(output_dir, fname), l)
                    local_fpath.append(l)
        
        return local_fpath
    
    @task()
    def cleanup_doc_worker(res_fpaths_local, data_on_worker, **kwargs):
        """This task deletes all the files from the docker worker

        # Args:
        #     fnames (list): the result files to be deleted on the docker worker  
              data_on_worker (str): delete the folder with the user data from the docker worker
        """

        print(f"Using {DW_CONNECTION_ID} connection")
        ssh_hook = get_connection(conn_id=DW_CONNECTION_ID)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            d = os.path.join(WORKER_DATA_LOCATION, data_on_worker)
            print(f"Deleting directory {DW_CONNECTION_ID}:{d}")
            for f in sftp_client.listdir(d):
                sftp_client.remove(f)
            sftp_client.rmdir(d)
        
                
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
            
        
    
    data = extract()
    files = transform(data)
    data_location = move_to_docker_host(files)
    data_on_worker = run_container(data_location)
    ls_results(data_on_worker)
    res_fpaths = retrieve_res(data_on_worker)
    cleanup_doc_worker(res_fpaths, data_on_worker)
    errcode = stageout_results(res_fpaths)
    cleanup_local(errcode, res_fpaths)

    # data >> files >> data_locations >> output_fnames >> ls_results(output_fnames) >> files >> stageout_results(files) >> cleanup()
    
dag = docker_in_worker()

