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
    def __init__(self, cloud_id,usr,password,verify_certs=False):
        self.connect = Elasticsearch(cloud_id=cloud_id,
                                        http_auth=(usr,password),
                                        verify_certs=verify_certs)


    def info(self):
        try:
            info = self.connect.info()
        except Exception as e:
            raise Exception(str(e))
        return info



    def add_index(self,index,id,body):
        try:
            index = self.connect.index(index=index,
                                       id=id,
                                       body=body)

        except Exception as e:
            raise Exception(str(e))




    def delete_index(self,index):
        try:
            getrepository = self.connect.delete(index=index,ignore=[405,404])

        except Exception as e:
            raise Exception(str(e))
        return getrepository


    def snapshot_get_repository(self,repository,master_timeout=None,local=None):
        try:
            getrepository = self.connect.snapshot.get_repository(repository=repository,
                                                     local=local,
                                                     master_timeout=master_timeout)

        except Exception as e:
            raise Exception(str(e))
        return getrepository


    def snapshot_clean_repository(self,repository,master_timeout=None,timeout=None):
        try:
            self.connect.snapshot.cleanup_repository(repository=repository,
                                                         master_timeout=master_timeout,
                                                         timeout=timeout)
        except Exception as e:
            raise Exception(str(e))




    def snapshot_create_repository(self,repository,repository_body,master_timeout=None):
        try:
            self.connect.snapshot.create_repository(repository=repository,
                                                        body=repository_body,
                                                        master_timeout=master_timeout)
        except Exception as e:
            raise Exception(str(e))



    def get_snapshot(self,repository,snapshot,verbose=True,master_timeout=False):
        try:
            getsnapshot = self.connect.snapshot.get_snapshot(repository=repository,
                                                   snapshot=snapshot,
                                                   master_timeout=master_timeout,
                                                   verbose=verbose)
        except Exception as e:
            raise Exception(str(e))
        return getsnapshot



    def create_snapshot(self,repository,snapshot,body,master_timeout=None):
        try:
            return self.connect.snapshot.create(repository=repository,
                                                snapshot=snapshot+str(datetime.now()).replace(" ",''),
                                                body=body,
                                                master_timeout=master_timeout)

        except Exception as e:
            raise Exception(str(e))




    def snapshot_delete(self):
        try:
            self.connect.snapshot.delete(repository=self.snapshot_delete["repository"],
                                         snapshot=self.snapshot_delete["snapshot"],
                                         master_timeout=self.snapshot_delete["master_timeout"])
        except Exception as e:
            raise Exception(str(e))


    def snapshot_restore(self,repository,snapshot,body=None,master_timeout=None):
        try:
            restore = self.connect.snapshot.restore(repository=repository,
                                                    snapshot=snapshot,
                                                    body=body,
                                                    master_timeout=master_timeout)
        except Exception as e:
            raise Exception(str(e))


    def delta_snapshot(self, index_name, start_time, end_time):
        #TODO
        print()


def parse_configuration(confFile):
    config = configparser.ConfigParser()
    with codecs.open(confFile, 'r') as f:
        config.read_file(f)

    conection=dict()
    for ops in config.options("conection"):
        if config.get('conection', ops)== "":
            conection[ops] = None
        else:
            conection[ops]=config.get('conection', ops)
    return conection


if __name__ == '__main__':
    sm = parse_configuration('snap.ini')
    a=SnapshotManager(sm["cloud_id"],sm["usr"],sm["password"])




    '''body have to be a dict'''

   a.add_index(sm["index_name"],int(sm["id"]),json.loads(sm["index_body"]))
   a.create_snapshot(sm["repository_name"],sm["snapshot_prefix"],{"indices":sm["index_name"],"query":{"range" : {"timestamp":{"gte":"2020-04-2302:11:40.633737","lte":"now"}}}})
   a.delete_index(sm["index_name"])
  a.snapshot_restore(sm["repository_name"],sm["snapshot_prefix"],{"indices":sm["index_name"],"include_global_state":True,"rename_pattern":sm["index_name"],"rename_replacement":sm["index_restore_name"]})


