########################################################################################################
#
#   This module has various hdfs utilities for use locally or via pyspark
#
###############
# For avro support install `fastavro` and use
# `from hdfs.ext.avro import AvroReader, AvroWriter`
###############

import os
import sys
import json
import hdfs as fs
import getpass

def client(host=luigi.configuration.get_config().get('hdfs','namenode'),
           port=luigi.configuration.get_config().get('hdfs','port'), 
           user=getpass.getuser()):
    return fs.InsecureClient('http://{0}:{1}'.format(host,port), user=user)


class Directory:
    def __init__(self, directory, client=client()):
        self.directory = directory
        self.client = client
        if self.status:
            self.exists = True
            self.storagePolicy = self.status['storagePolicy']
            self.accessTime = self.status['accessTime']
            self.type = self.status['type']
            self.fileId = self.status['fileId']
            self.group = self.status['group']
            self.permission = self.status['permission']
            self.length = self.status['length']
            self.childrenNum = self.status['childrenNum']
            self.blockSize = self.status['blockSize']
            self.owner = self.status['owner']
            self.pathSuffix = self.status['pathSuffix']
            self.replication = self.status['replication']
            self.modificationTime = self.status['modificationTime']
        else:
            self.exists = False

    def list(self):
        return self.client.list(self.directory)

    @property
    def status(self):
        return self.client.status(self.directory)

    @property
    def summary(self):
        return self.client.content(self.directory)

    def move(self, new):
        return self.client.rename(self.directory, new)

    def delete(self, recursive=True):
        # by default, skips trash
        return self.client.delete(self.directory, recursive)

    def download(self, local, n_threads=5):
        return self.client.download(self.directory, local, n_threads)


class File(Directory):

    def read(self, json=False):
        if json:
          return json.load(self.client.read(self.directory, encoding='utf-8'))
        return self.client.read(self.directory).read()

    def read_stream(self, chunk_size=8096, delimiter='\n'):
        with self.client.read(self.directory, chunk_size=chunk_size, delimiter=delimiter) as reader:
            for chunk in reader: 
                pass

    def write(self, data, json=False):
        if json:
          with self.client.write(self.directory, encoding='utf-8') as writer:
            json.dump(data, writer)
        with self.client.write(self.directory) as writer:
          writer.write(data)
