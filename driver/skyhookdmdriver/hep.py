from skyhookdmdriver.skyhook_common import *
from skyhookdmdriver.skyhook_driver import *

def writeRootDataset(file_urls, dstname, addr, ceph_pool):

    def match_root_datatype(d_type):
        field_types = {}
        field_types[None] = 0
        field_types['int8'] = 1
        field_types['int16'] = 2
        field_types['int32'] = 3
        field_types['int64'] = 4
        field_types['uint8'] = 5
        field_types['uint16'] = 6
        field_types['uint32'] = 7
        field_types['uint64'] = 8
        field_types['char'] = 9
        field_types['uchar'] = 10
        field_types['bool'] = 11
        field_types['float'] = 12
        field_types['float64'] = 13
        field_types['date'] = 14
        field_types['string'] = 15
        field_types['[0, inf) -> bool'] = 16
        field_types['[0, inf) -> char'] = 17
        field_types['[0, inf) -> uchar'] = 18
        field_types['[0, inf) -> int8'] = 19
        field_types['[0, inf) -> int16'] = 20
        field_types['[0, inf) -> int32'] = 21
        field_types['[0, inf) -> int64'] = 22
        field_types['[0, inf) -> uint8'] = 23
        field_types['[0, inf) -> uint16'] = 24
        field_types['[0, inf) -> uint32'] = 25
        field_types['[0, inf) -> uint64'] = 26
        field_types['[0, inf) -> float32'] = 27
        field_types['[0, inf) -> float64'] = 28

        if str(d_type) in field_types.keys():
            return field_types[str(d_type)]

        return 0

    def buildRootObj(dst_name, branch, subnode, obj_id):
        from collections import OrderedDict
        brname = branch.name.decode("utf-8")
        objname = '.' + brname + '.' + str(obj_id)
        parent = subnode.parent
        while parent is not None:
            objname = parent.name + '#' + objname
            parent = parent.parent

        objname = dst_name + '#' + objname

        # this is for the event id colo
        event_id_col = pa.field('EVENT_ID', pa.int64())
        id_array = range(branch.numentries)

        # they don't care about this for now. but just leave it here for future use.
        field = None
        fieldmeta = {}
        fieldmeta['BasketSeek'] = bytes(branch._fBasketSeek)
        fieldmeta['BasketBytes'] = bytes(branch._fBasketBytes)
        fieldmeta['Compression'] = bytes(str(branch.compression),'utf8')
        fieldmeta['Compressionratio'] = bytes(str(branch.compressionratio()),'utf8')

        if('inf' not in str(branch.interpretation.type) and 'bool' in str(branch.interpretation.type)):
            function=getattr(pa,'bool_')
            field = pa.field(branch.name.upper(), function(), metadata = fieldmeta)

        elif('inf' in str(branch.interpretation.type)):
            function= getattr(pa,'list_')
            subfunc = None
            if('bool' in str(branch.interpretation.type)):
                subfunc = getattr(pa,'bool_')
            else:
                subfunc = getattr(pa,str(branch.interpretation.type).split()[-1])
            field = pa.field(branch.name.upper(), function(subfunc()), metadata = fieldmeta)

        else:
            try:
                function=getattr(pa,str(branch.interpretation.type))
                field = pa.field(branch.name.upper(), function(), metadata = fieldmeta)
            except Exception as e:
                print(str(e))
                field = pa.field(branch.name.upper(), pa.float64(), metadata = fieldmeta)


        schema = pa.schema([event_id_col, field])
        data_schema = '0 4 0 0 EVENT_ID;' + str(obj_id) + ' ' + str(match_root_datatype(branch.interpretation.type)) + ' 0 1 ' + branch.name.decode("utf-8").upper()
        meta_obj = TableMeta('0', '1', '2', SkyFormatType.SFT_ARROW, data_schema, 'n/a', str(branch.name), str(branch.numentries))
        sche_meta = meta_obj.getTableMeta()
        
        schema = schema.with_metadata(sche_meta)
        table = pa.Table.from_arrays([id_array, branch.array().tolist()],schema = schema)
        batches = table.to_batches()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, schema)

        for batch in batches:
            writer.write_batch(batch)
        buff = sink.getvalue()
        buff_bytes = buff.to_pybytes()

        size_limit = 238000000

        num_partitions = 1

        if len(buff_bytes) > size_limit:
            total_rows = len(table.columns[0])
            num_partitions = len(buff_bytes)/size_limit
            num_partitions += 1
            batch_size = total_rows/num_partitions
            batches = table.to_batches(batch_size)

        try:
            if num_partitions == 1:
            # Write to the Ceph pool
                buff_bytes = addFB_Meta(buff_bytes)
                writeToCeph(buff_bytes, objname + '.0', ceph_pool)
            else:
                i = 0
                for batch in batches:
                    sink = pa.BufferOutputStream()
                    writer = pa.RecordBatchStreamWriter(sink, schema)
                    writer.write_batch(batch)
                    buff = sink.getvalue()
                    buff_bytes = buff.to_pybytes()
                    buff_bytes = addFB_Meta(buff_bytes)
                    writeToCeph(buff_bytes, objname + '.' + str(i), ceph_pool)

        except Exception as e:
            print(str(e))

    def growTree(dst_name, node, rootobj):

        if 'allkeys' not in dir(rootobj):
            return

        child_id = 0

        data_schema = None
        if ('TTree' in str(node.classtype)):
            data_schema = ''

        for key in rootobj.allkeys():

            datatype = None
            if 'Branch' in str(type(rootobj[key])):
                datatype = str(rootobj[key].interpretation.type)

            subnode = RootNode(key.decode("utf-8"),str(type(rootobj[key])).split('.')[-1].split('\'')[0], datatype, node, child_id, None)
            node.children.append(subnode)
            growTree(dst_name, subnode, rootobj[key])

            #build the object if it's a branch
            if('Branch' in str(subnode.classtype)):
                try:
                    buildRootObj(dst_name, rootobj[key], subnode, child_id)
                except Exception as e:
                    print(str(e))
                    print(subnode)

                subnode.data_schema = str(child_id) + ' ' + str(match_root_datatype(subnode.datatype)) + ' 0 1 ' + subnode.name
                data_schema = data_schema + str(child_id) + ' ' + str(match_root_datatype(subnode.datatype)) + ' 0 1 ' + subnode.name + '; '

            child_id += 1

        node.data_schema = data_schema


    def treeTraversal(root):
        output = {}
        if root!=None:
            children = root.children
        output["name"] = str(root.name)
        output["classtype"] = str(root.classtype)
        output['datatype'] = str(root.datatype)
        output['node_id'] = str(root.node_id)
        output['data_schema'] = str(root.data_schema)
        output["children"] = []

        for node in children:
            output["children"].append(treeTraversal(node))
        return output

    def processRootFile(url):
        import wget
        if 'http' in url:
            filename = wget.download(url)
        else:
            filename = url

        root_dir = uproot.open(filename)
        stat_res = os.stat(filename)
        stat_res_dict = {}
        stat_res_dict['size'] = str(stat_res.st_size)
        stat_res_dict['last access time'] = str(stat_res.st_atime)
        stat_res_dict['last modified time'] = str(stat_res.st_mtime)
        stat_res_dict['last changed time'] = str(stat_res.st_ctime)
        stat_res_dict['name'] = str(filename)

        tree = RootNode(root_dir.name.decode("utf-8"), str(type(root_dir)).split('.')[-1].split('\'')[0], None, None, 0, None)
        growTree(dstname + '#' + filename, tree, root_dir)
        logic_schema = treeTraversal(tree)
        os.remove(filename)
        res = [stat_res_dict, logic_schema]
        return res

    client = Client(addr)

    file_list = file_urls
    
    metadata = {}
    metadata['dataset_name'] = dstname
    metadata['files'] = []

    #the size of the dataset
    total_size = 0

    futures = []
    for r_file in file_list:
        file_meta = {}
        file_meta['name'] = r_file
        file_meta['ROOTDirectory'] = uproot.open(r_file).name.decode("utf-8")
        future = client.submit(processRootFile, r_file)

        futures.append(future)

        metadata['files'].append(file_meta)

    res = client.gather(futures)

    i = 0
    for item in metadata['files']:
        item['file_schema'] = res[i][1]
        item['file_attributes'] = res[i][0]
        item['name'] = res[i][0]['name']
        total_size += int(res[i][0]['size'])
        i += 1

    metadata['size'] = total_size

    output = json.dumps(metadata,indent=4)
    writeToCeph(bytes(output,'utf-8'), dstname, ceph_pool)

    return True