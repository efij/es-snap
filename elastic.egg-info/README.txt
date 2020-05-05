Library used for wrapping elasticsearch.py code (https://elasticsearch-py.readthedocs.io/en/master/)

function:

    connect : make coneection to your elasticsearch account.
            to make it work, you have to add your user_name and password to the "snap.ini" file.
            the function return SnapshotManager object (See details below in class SnapshotManager)
            NOTE: you have to call that function.

    parse_configuration : you can use this function if you have an ini file.
                        this function will parser your file to dict.
                        return your new ini dict
                        NOTE: your all declaration will have to be under [conection] ops/key

    parser : use for cli.
       option:
         info : basic information about the cluster. not expect pram
         snap : name of snapshot. expect repository name,snapshot name,body(has to be a dict)
         rest : restore snapshot. expect repository name,snapshot name
         repos : create repository. expect repository name, repository_body(has to be a dict)
         i : create index. expect index name, doc id, body(has to be a dict)
         di : delete index. expect index name
         dsnap : delete snapshot. expect repository name,snapshot name
         ddoc : delete doc from index. expect index name , doc id




class:

    class SnapshotManager:
        __init__(self, usr, password, verify_certs=False) : make the connect to elasticsearch
        info(self) : bring basic information about the cluster
        add_index(self, index, id, body) : add new index to the db
        delete_index(self, index) : delete index from the db
        delete_doc(self, index, id) : delete spesific doc from the db
        exists_index(self, index) : check if index exists
        snapshot_create_repository(self, repository, repository_body, master_timeout=None) : create new repository
        create_snapshot(self, repository, snapshot, body=None) : create new snapshot
        snapshot_delete(self,repository,snapshot,master_timeout=None) : delete snapshot from db
        snapshot_restore(self, repository, snapshot, body=None, master_timeout=None) restore snapshot
        NOTE: THE BODY PARAMETER HAS TO BE DICT!!


IF YOU WANT TO RUN SOME FUNCTION EVERYDAY :
1) To edit a crontab entries, use crontab -e.
2)ADD : {time} * * * {file_loction} {parser parameter depending on the function you want to run}
NOTE: THE TIME NEED TO BE - seconds  minutes, (00 09 mean 9 AM)