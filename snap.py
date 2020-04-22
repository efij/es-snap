#!/usr/bin/env python3
# encoding: utf-8

from elasticsearch import Elasticsearch
import urllib3
from datetime import datetime
import configparser

urllib3.disable_warnings()
import codecs


# host,port,url_prefix,use_sslverify_certs,ca_certs,client_cert,client_key

class SnapshotManager:
    def __init__(self, config_path):
        self.config = config_path

    def parse_configuration(self):
        config = configparser.ConfigParser()
        with codecs.open('a.ini', 'r') as f:
            config.read_file(f)

        conection = dict()
        conection["cloud_id"] = config.get('conection', 'cloud_id')
        conection["usr"] = config.get('conection', 'usr')
        conection["password"] = config.get('conection', 'password')
        conection["verify_certs"] = config.get('conection', 'verify_certs')
        for i in conection:
            if conection[i] == "":
                coneection[i] = None

        cleanup_repository = dict()
        cleanup_repository["repository"] = config.get('cleanup_repository', 'repository')
        cleanup_repository["master_timeout"] = config.get('cleanup_repository', 'repository')
        cleanup_repository["timeout"] = config.get('cleanup_repository', 'repository')
        for i in cleanup_repository:
            if cleanup_repository[i] == "":
                cleanup_repository[i] = None

        get_repository = dict()
        get_repository["repository"] = config.get('get_repository', 'repository')
        get_repository["local"] = config.get('get_repository', 'local')
        get_repository["master_timeout"] = config.get('get_repository', 'master_timeout')
        for i in get_repository:
            if get_repository[i] == "":
                get_repository[i] = None

        create_repository = dict()
        create_repository["repository"] = config.get('create_repository', 'repository')
        create_repository["body"] = {"type": config.get('create_repository', 'type'), "settings": {
            config.get('create_repository', 'type'): config.get('create_repository', 'location')}}
        create_repository["compress"] = config.get('create_repository', 'compress')
        create_repository["master_timeout"] = config.get('create_repository', 'master_timeout')
        create_repository["timeout"] = config.get('create_repository', 'timeout')
        create_repository["verify"] = config.get('create_repository', 'verify')
        for i in create_repository:
            if create_repository[i] == "":
                create_repository[i] = None

        delete_repository = dict()
        delete_repository["repository"] = config.get('delete_repository', 'repository')
        delete_repository["master_timeout"] = config.get('delete_repository', 'master_timeout')
        delete_repository["timeout"] = config.get('delete_repository', 'timeout')
        for i in delete_repository:
            if delete_repository[i] == "":
                delete_repository[i] = None

        get_snapshot = dict()
        get_snapshot["repository"] = config.get('get_snapshot', 'repository')
        get_snapshot["snapshot"] = config.get('get_snapshot', 'snapshot')
        get_snapshot["ignore_unavailable"] = config.get('get_snapshot', 'ignore_unavailable')
        get_snapshot["master_timeout"] = config.get('get_snapshot', 'master_timeout')
        get_snapshot["verbose"] = config.get('get_snapshot', 'verbose')
        for i in get_snapshot:
            if get_snapshot[i] == "":
                get_snapshot[i] = None

        create_snapshot = dict()
        create_snapshot["repository"] = config.get('create_snapshot', 'repository')
        create_snapshot["snapshot"] = config.get('create_snapshot', 'snapshot')
        create_snapshot["body"] = {"indices": config.get('create_snapshot', 'indices'),
                                   "ignore_unavailable": config.get('create_snapshot', 'ignore_unavailable'),
                                   "include_global_state": config.get('create_snapshot', 'include_global_state')}
        create_snapshot["master_timeout"] = config.get('create_snapshot', 'master_timeout')
        create_snapshot["wait_for_completion"] = config.get('create_snapshot', 'wait_for_completion')
        for i in create_snapshot:
            if create_snapshot[i] == "":
                create_snapshot[i] = None

        snapshot_delete = dict()
        snapshot_delete["repository"] = config.get('snapshot_delete', 'repository')
        snapshot_delete["snapshot"] = config.get('snapshot_delete', 'snapshot')
        snapshot_delete["master_timeout"] = config.get('snapshot_delete', 'master_timeout')
        for i in snapshot_delete:
            if snapshot_delete[i] == "":
                snapshot_delete[i] = None

        snapshot_restore = dict()
        snapshot_restore["repository"] = config.get('snapshot_restore', 'repository')
        snapshot_restore["snapshot"] = config.get('snapshot_restore', 'snapshot')
        snapshot_restore["body"] = config.get('snapshot_restore', 'body')
        snapshot_restore["master_timeout"] = config.get('snapshot_restore', 'master_timeout')
        snapshot_restore["wait_for_completion"] = config.get('snapshot_restore', 'wait_for_completion')
        for i in snapshot_restore:
            if snapshot_restore[i] == "":
                snapshot_restore[i] = None

    def connect(self):
        connect = Elasticsearch(cloud_id=conection["cloud_id"],
                                http_auth=(conection["usr"], conection["password"]),
                                verify_certs=conection["verify_certs"])

    def info(self):
        try:
            x = self.connect.info()
        except Exception as e:
            print(e)
        return x

    # ,repository=get_repository["repository"], local=get_repository["local"], master_timeout=get_repository["master_timeout"]
    def snapshot_get_repository(self):
        try:
            x = self.connect.snapshot.get_repository(repository=self.get_repository["repository"],
                                                     local=self.get_repository["local"],
                                                     master_timeout=self.get_repository["master_timeout"])

        except Exception as e:
            print(e)
        return x

    # ,repository=cleanup_repository["repository"],master_timeout=cleanup_repository["master_timeout"],timeout=cleanup_repository["timeout"]
    def snapshot_clean_repository(self):
        try:
            x = self.connect.snapshot.cleanup_repository(repository=self.cleanup_repository["repository"],
                                                         master_timeout=self.cleanup_repository["master_timeout"],
                                                         timeout=self.cleanup_repository["timeout"])
        except Exception as e:
            print(e)
        return x

    # ,repository=create_repository["repository"],snapshot=create_repository["body"],body=create_repository["master_timeout"],master_timeout=create_repository["master_timeout"]
    def snapshot_create_repository(self):
        try:
            w = self.connect.snapshot.create_repository(repository=self.create_repository["repository"],
                                                        snapshot=self.create_repository["snapshot"],
                                                        body=self.create_repository["body"],
                                                        master_timeout=self.create_repository["master_timeout"])
        except Exception as e:
            print(e)
        return w

    # ,repository=get_snapshot["repository"],snapshot=get_snapshot["snapshot"],ignore_unavailable=get_snapshot["ignore_unavailable"],master_timeout=get_snapshot["master_timeout"],verbose=get_snapshot["verbose"]
    def get_snapshot(self):
        try:
            w = self.connect.snapshot.get_snapshot(repository=self.get_snapshot["repository"],
                                                   snapshot=self.get_snapshot["snapshot"],
                                                   ignore_unavailable=self.get_snapshot["ignore_unavailable"],
                                                   master_timeout=self.get_snapshot["master_timeout"],
                                                   verbose=self.get_snapshot["verbose"])
        except Exception as e:
            print(e)
        return w

    # ,repository=create_snapshot["repository"], snapshot=create_snapshot["snapshot"], body=create_snapshot["body"],master_timeout=create_snapshot["master_timeout"],wait_for_completion=create_snapshot["wait_for_completion"]
    def create_snapshot(self):
        try:
            return self.connect.snapshot.create(repository=self.create_snapshot["repository"],
                                                snapshot=self.create_snapshot["snapshot"],
                                                body=self.create_snapshot["body"],
                                                master_timeout=self.create_snapshot["master_timeout"],
                                                wait_for_completion=self.create_snapshot["wait_for_completion"])
        except Exception as e:
            raise Exception(str(e))

    # ,snapshot=snapshot_delete["snapshot"],repository=snapshot_delete["repository"],master_timeout=snapshot_delete["master_timeout"]
    def snapshot_delete(self):
        try:
            self.connect.snapshot.delete(repository=self.snapshot_delete["repository"],
                                         snapshot=self.snapshot_delete["snapshot"],
                                         master_timeout=self.snapshot_delete["master_timeout"])
        except Exception as e:
            print(e)

    '''
     ,repository=snapshot_restore["repository"], snapshot=snapshot_restore["snapshot"],body=snapshot_restore["body"],master_timeout=snapshot_restore["master_timeout"],wait_for_completion=snapshot_restore["wait_for_completion"]
    '''
    def snapshot_restore(self):
        try:
            restore = self.connect.snapshot.restore(repository=self.snapshot_restore["repository"],
                                                    snapshot=self.snapshot_restore["snapshot"],
                                                    body=self.snapshot_restore["body"],
                                                    master_timeout=self.snapshot_restore["master_timeout"],
                                                    wait_for_completion=self.snapshot_restore["wait_for_completion"])
        except Exception as e:
            print(e)
        return restore

    def delta_snapshot(self, index_name, start_time, end_time):
        #TODO
        print()

if __name__ == '__main__':
    sm = SnapshotManager('snap.ini')
    # print(sm.info())
    # print(sm.get_repository["repository"])
    # print(sm.snapshot_get_repository())
    # print(a.create_snapshot())

    # 0. Fill ES db index

    # 1. Snapshot Index

    # 2. Delete Index

    # 3. Restore Index
