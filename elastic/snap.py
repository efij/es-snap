#!/usr/bin/env python3
# encoding: utf-8

from elasticsearch import Elasticsearch
import urllib3
from datetime import datetime
import configparser
import codecs
import argparse
import pathlib

import json

urllib3.disable_warnings()


class SnapshotManager:
    def __init__(self, usr, password, verify_certs=False):
     self.connect = Elasticsearch( http_auth=(usr, password),
                                  verify_certs=verify_certs,timeout=800)

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

    def snapshot_create_repository(self, repository, repository_body, master_timeout=None):
        try:
            self.connect.snapshot.create_repository(repository=repository,
                                                    body=repository_body,
                                                    master_timeout=master_timeout)
        except Exception as e:
            raise Exception(str(e))

    def create_snapshot(self, repository, snapshot, body=None):
        try:
            name = snapshot + str(datetime.now()).replace(" ", '')
            return self.connect.snapshot.create(repository=repository,
                                                 snapshot=name,
                                                 body=body,
                                                 wait_for_completion=True)
        except Exception as e:
            raise Exception(str(e))

    def snapshot_delete(self,repository,snapshot,master_timeout=None):
        try:
            self.connect.snapshot.delete(repository=repository,
                                         snapshot=snapshot,
                                         master_timeout=master_timeout)
        except Exception as e:
            raise Exception(str(e))

    def snapshot_restore(self, repository, snapshot, body=None, master_timeout=None):
        closeindex = []
        try:
            openindex = self.connect.snapshot.get(repository=repository, snapshot=snapshot)['snapshots'][0][
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

def connect():
    path=str(pathlib.Path(__file__).parent.absolute())+'\snap.ini'
    try:
        config = configparser.ConfigParser()
        with codecs.open(path, 'r') as f:
            config.read_file(f)

        a = SnapshotManager(config.get('conection',"usr"), config.get('conection',"password"))
        return a
    except Exception as e:
        raise Exception(str(e))

def parser():

    parser = argparse.ArgumentParser(description="A text file manager!")

    parser.add_argument("-info",nargs="?",const="info",
                        help="basic information about the cluster")

    parser.add_argument("-snap", "--snapshot", nargs=3,
                        metavar=["repository", "snapshot", "body"],
                        help="name of snapshot")


    parser.add_argument("-rest", "--restore",nargs=2,
                        metavar=("repository", "snapshot"), default=None,
                        help="restore snapshot")

    parser.add_argument("-repos", "--repository", type=str, nargs=2,
                        metavar=("repository", "repository_body"), default=None,
                        help="create repository")

    parser.add_argument("-i", "--index", nargs=3,
                        metavar=["index", "id", "body"], default=None,
                        help="create index")


    parser.add_argument("-di", "--dindex", type=str, nargs=1,
                        metavar="index", default=None,
                        help="delete index")

    parser.add_argument("-dsnap", "--dsnapshot", type=str, nargs=2,
                        metavar=("repository", "snapshot"), default=None,
                        help="delete snapshot")

    parser.add_argument("-ddoc", "--ddoc", nargs=2,
                        metavar=("index", "doc"), default=None,
                        help="delete doc from index")



    args = parser.parse_args()

    if args.info:
        print(a.info())
    elif args.snapshot:
        a.create_snapshot(args.snapshot[0],args.snapshot[1],args.snapshot[2])
    elif args.restore:
        a.snapshot_restore(args.restore[0],args.restore[1])
    elif args.repository:
        a.snapshot_create_repository(args.repository[0],args.repository[1])
    elif args.index:
        a.add_index(args.index[0],int(args.index[1]),args.index[2])
    elif args.dindex:
        a.delete_index(args.dindex[0])
    elif args.dsnapshot:
        a.snapshot_delete(args.dsnapshot[0],args.dsnapshot[1])
    elif args.ddoc:
        a.delete_doc(args.ddoc[0],int(args.ddoc[1]))


if __name__ == '__main__':
    a = connect()
    parser()
