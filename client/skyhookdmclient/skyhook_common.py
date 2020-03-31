import os
import json
import uproot
import dask.delayed
import pyarrow as pa
from pyarrow import csv
from os import listdir
from os.path import isfile, join
from dask.distributed import Client


class Dataset:
    def __init__(self, name, size, files):
        self.name  = name
        self.size = size
        self.files = files
        
    def getFiles(self):
        return self.files
    
    def getSize(self):
        return self.size
    
    def _runQuery(self, querystr):
        print(querystr)
        
    def __str__(self):
        return '\"Dataset: ' + self.name + ', ' + str(self.size) + ' bytes\"'
    
    def __repr__(self):
        return '\"Dataset: ' + self.name + ', ' + str(self.size) + ' bytes\"'


class File:
    def __init__(self, name, attributes, schema, dataset, rootdirectory):
        self.name = name
        self.attributes = attributes
        self.schema = schema
        self.dataset = dataset
        self.ROOTDirectory = rootdirectory
        
    def getAttributes(self):
        return self.attributes
    

    def getRoot(self):
        node = self._buildTree(self.schema, None)
        return node
    
    def getSchema(self):
        return self.schema
    
    
    def _buildTree(self, nd_dict, parent):
        node = RootNode(nd_dict['name'], nd_dict['classtype'], nd_dict['datatype'], parent, nd_dict['node_id'], nd_dict['data_schema'])
        node.children = []
        for item in nd_dict['children']:
            tmp = self._buildTree(item, node)
            node.children.append(tmp)
        return node
    
    def _runQuery(self, querystr):
        obj_prefix = self.dataset + '.' + self.name
        print(obj_prefix)
        
    def __str__(self):
        return '\"File: ' + self.name + ', ' + str(self.attributes['size']) + ' bytes\"\n'
    
    def __repr__(self):
        return '\"File: ' + self.name + ', ' + str(self.attributes['size']) + ' bytes\"\n'


class RootNode(object):
    def __init__(self, name, classtype, datatype, parent, node_id, data_schema):
        self.children  = []
        self.name = name
        self.classtype = classtype
        self.parent = parent
        self.datatype = datatype
        self.node_id = node_id
        self.data_schema = data_schema
        
    def getName(self):
        return self.name
    
    def getClassType(self):
        return self.classtype
    
    def getDataType(self):
        return self.datatype
    
    def getChildren(self):
        #for child in self.children:
            #print(child.classtype + ': ' + child.name + ', ' + child.datatype)
        return self.children
    
    def getParent(self):
        #print(self.parent.classtype + ': ' + self.parent.name + ', ' + self.parent.datatype)
        return self.parent
    
    def __str__(self):
        if self.parent != None:
            return '\"RootNode: id: '+ str(self.node_id) + ', ' + str(self.classtype) + ': ' + str(self.parent.name) + '.' + str(self.name) + ', ' + str(self.datatype) +'\"\n'
        return '\"RootNode: id: '+ str(self.node_id) + ', ' + str(self.classtype) + ': ' + self.name + ', ' + str(self.datatype) +'\"\n'
    
    def __repr__(self):
        if self.parent != None:
            return '\"RootNode: id: '+ str(self.node_id) + ', ' + str(self.classtype) + ': ' + self.parent.name + '.' + self.name + ', ' + str(self.datatype) +'\"\n'
        return '\"RootNode: id: '+ str(self.node_id) + ', ' + str(self.classtype) + ': ' + self.name + ', ' + str(self.datatype) +'\"\n'
