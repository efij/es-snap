#!/usr/bin/env python3
# encoding: utf-8

from elasticsearch import Elasticsearch
import urllib3
from datetime import datetime
import configparser
import codecs
import json

urllib3.disable_warnings()


class SnapshotManager:
    def __init__(self, cloud_id, usr, password, verify_certs=False):
        self.connect = Elasticsearch(cloud_id=cloud_id,
                                     http_auth=(usr, password),
                                     verify_certs=verify_certs)

    def info(self):
        try:
            info = self.connect.info()
            return info
        except Exception as e:
            raise Exception(str(e))

    def add_index(self, index, id, body):
        try:
            index = self.connect.index(index=index,
                                       id=id,
                                       body=body)
        except Exception as e:
            raise Exception(str(e))

    def delete_index(self, index):
        try:
            self.connect.indices.delete(index=index)
        except Exception as e:
            raise Exception(str(e))

    def delete_doc(self, index, id):
        try:
            self.connect.delete(index=index, id=id)
        except Exception as e:
            raise Exception(str(e))

    def exists_index(self, index):
        try:
            se = self.connect.indices.exists(index=index)
            return se
        except Exception as e:
            raise Exception(str(e))

    def snapshot_get_repository(self, repository, master_timeout=None, local=None):
        try:
            getrepository = self.connect.snapshot.get_repository(repository=repository,
                                                                 local=local,
                                                                 master_timeout=master_timeout)
            return getrepository
        except Exception as e:
            raise Exception(str(e))

    def snapshot_clean_repository(self, repository, master_timeout=None, timeout=None):
        try:
            self.connect.snapshot.cleanup_repository(repository=repository,
                                                     master_timeout=master_timeout,
                                                     timeout=timeout)
        except Exception as e:
            raise Exception(str(e))

    def snapshot_create_repository(self, repository, repository_body, master_timeout=None):
        try:
            self.connect.snapshot.create_repository(repository=repository,
                                                    body=repository_body,
                                                    master_timeout=master_timeout)
        except Exception as e:
            raise Exception(str(e))

    def get_snapshot(self, repository, snapshot, verbose=True, master_timeout=False):
        try:
            getsnapshot = self.connect.snapshot.get_snapshot(repository=repository,
                                                             snapshot=snapshot,
                                                             master_timeout=master_timeout,
                                                             verbose=verbose)
            return getsnapshot
        except Exception as e:
            raise Exception(str(e))

    def create_snapshot(self, repository, snapshot, body, master_timeout=None):
        try:
            name = snapshot + str(datetime.now()).replace(" ", '')
            self.connect.snapshot.create(repository=repository,
                                         snapshot=name,
                                         body=body,
                                         master_timeout=master_timeout,
                                         wait_for_completion=True)
            return name
        except Exception as e:
            raise Exception(str(e))

    def snapshot_delete(self):
        try:
            self.connect.snapshot.delete(repository=self.snapshot_delete["repository"],
                                         snapshot=self.snapshot_delete["snapshot"],
                                         master_timeout=self.snapshot_delete["master_timeout"])
        except Exception as e:
            raise Exception(str(e))

    def snapshot_restore(self, repository, snapshot, body=None, master_timeout=None):
        closeindex = []
        try:
            openindex = self.connect.snapshot.get(repository=sm["repository_name"], snapshot=snapshot)['snapshots'][0][
                'indices']
            for i in openindex:
                if self.exists_index(i):
                    self.connect.indices.close(index=i)
                    closeindex.append(i)
            restore = self.connect.snapshot.restore(repository=repository,
                                                    snapshot=snapshot,
                                                    body=body,
                                                    master_timeout=master_timeout)
            for i in closeindex:
                self.connect.indices.open(index=i)
        except Exception as e:
            raise Exception(str(e))

    def delta_snapshot(self, index_name, start_time, end_time):
        # TODO
        print()


def parse_configuration(confFile):
    config = configparser.ConfigParser()
    with codecs.open(confFile, 'r') as f:
        config.read_file(f)

    conection = dict()
    for ops in config.options("conection"):
        if config.get('conection', ops) == "":
            conection[ops] = None
        else:
            conection[ops] = config.get('conection', ops)
    return conection


if __name__ == '__main__':
    sm = parse_configuration('snap.ini')
    z = parse_configuration(r'C:\Users\shiri\Desktop\ss.ini')
    a = SnapshotManager(z["cloud_id"], z["usr"], z["password"])

'''example for snapshot_restore: a.snapshot_restore(sm["repository_name"],"snap_2020-04-2403:09:57.096488",{"index_settings":{"include_global_state":True}})'''
'''example for create_snapshot: a.create_snapshot(sm["repository_name"],sm["snapshot_prefix"],{"indices":sm["index_name"]'''
'''body have to be a dict'''
