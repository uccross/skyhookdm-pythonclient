import os
import json
import enum 
import uproot
import flatbuffers
import dask.delayed
import pyarrow as pa
from pyarrow import csv
from os import listdir
from os.path import isfile, join
from dask.distributed import Client
from skyhookdmdriver.Tables import FB_Meta

class SkyFormatType(enum.Enum): 
    SFT_FLATBUF_FLEX_ROW = 1
    SFT_FLATBUF_UNION_ROW = 2
    SFT_FLATBUF_UNION_COL = 3
    SFT_FLATBUF_CSV_ROW = 4
    SFT_ARROW = 5
    SFT_PARQUET = 6
    SFT_PG_TUPLE = 7
    SFT_CSV = 8
    SFT_JSON = 9
    SFT_PG_BINARY = 10
    SFT_PYARROW_BINARY = 11
    SFT_HDF5 = 12
    SFT_EXAMPLE_FORMAT = 13


class SkyDataType(enum.Enum): 
    SDT_INT8 = 1
    SDT_INT16 = 2
    SDT_INT32 = 3
    SDT_INT64 = 4
    SDT_UINT8 = 5
    SDT_UINT16 = 6
    SDT_UINT32 = 7
    SDT_UINT64 = 8 
    SDT_CHAR = 9
    SDT_UCHAR = 10
    SDT_BOOL = 11 
    SDT_FLOAT = 12
    SDT_DOUBLE = 13
    SDT_DATE = 14
    SDT_STRING = 15
    SDT_VECTOR_BOOL = 16
    SDT_VECTOR_CHAR = 17
    SDT_VECTOR_UCHAR = 18
    SDT_VECTOR_INT8 = 19
    SDT_VECTOR_INT16 = 20
    SDT_VECTOR_INT32 = 21
    SDT_VECTOR_INT64 = 22
    SDT_VECTOR_UINT8 = 23
    SDT_VECTOR_UNT16 = 24
    SDT_VECTOR_UINT32 = 25
    SDT_VECTOR_UINT64 = 26
    SDT_VECTOR_FLOAT = 27
    SDT_VECTOR_DOUBLE = 28

class TableMeta:

    def __init__(self, skyhook_version, data_schema_version, data_structure_version, data_format_type, data_schema, db_schema, table_name, num_rows):
        self.skyhook_version = skyhook_version
        self.data_schema_version = data_schema_version
        self.data_structure_version = data_structure_version
        self.data_format_type = str(data_format_type.value)
        self.data_schema = data_schema
        self.db_schema = db_schema
        self.table_name = table_name
        self.num_rows = num_rows

    def getTableMeta(self):
        sche_meta = {}
        sche_meta['skyhook_version'] = self.skyhook_version
        sche_meta['data_schema_version'] = self.data_schema_version
        sche_meta['data_structure_version'] = self.data_structure_version
        sche_meta['data_format_type'] = self.data_format_type
        sche_meta['data_schema'] = self.data_schema
        sche_meta['db_schema'] = self.db_schema
        sche_meta['table_name']= self.table_name
        sche_meta['num_rows'] = self.num_rows
        return sche_meta

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


def addFB_Meta(arrow_binary):

    builder = flatbuffers.Builder(0)

    wrapped_data_blob = builder.CreateByteVector(arrow_binary)

    FB_Meta.FB_MetaStart(builder)
    FB_Meta.FB_MetaAddBlobFormat(builder, 5)
    FB_Meta.FB_MetaAddBlobData(builder, wrapped_data_blob)
    FB_Meta.FB_MetaAddBlobSize(builder, len(arrow_binary))
    FB_Meta.FB_MetaAddBlobDeleted(builder, False)
    FB_Meta.FB_MetaAddBlobOrigOff(builder, 0)
    FB_Meta.FB_MetaAddBlobOrigLen(builder, len(arrow_binary))
    FB_Meta.FB_MetaAddBlobCompression(builder, 0)

    builder.Finish(FB_Meta.FB_MetaEnd(builder))

    data_bytes = bytes(builder.Output())

    data_len_bytes = len(data_bytes).to_bytes(4, byteorder='little')

    return data_len_bytes + data_bytes
