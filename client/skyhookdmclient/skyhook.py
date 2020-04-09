import struct
from skyhookdmclient.skyhook_common import *
import awkward

class SkyhookDM:
    def __init__(self):
        self.client = None
        self.addr = None

        # try:
        #     self.cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        #     self.cluster.connect()

        # except Exception as e:
        #     print(str(e))

    def connect(self, ip, ceph_pool_name):
        addr = ip+':8786'
        self.addr = addr
        client = Client(addr)
        self.client = client
        self.ceph_pool = ceph_pool_name
        # self.ioctx = self.cluster.open_ioctx(ceph_pool_name)

    # Write the arrow table to Ceph
    # Do we need to serialize it?
    # Exception Handling
    # Maintain Metadata
    # Object namming
    # Merge object?
    # Split object?
    # Submit to driver?
    def writeArrowTable(self, table, data_schema_name, table_name=''):
        
        def runOnDriver(buff_bytes, name,  ceph_pool):
            from skyhookdmdriver import skyhook_driver as sd
            res = sd.writeArrowTable(buff_bytes, name, ceph_pool)
            return res

        # In the servicex case, the data_schema_name is did for now
        # the data_schema_name is not checked so far. Will add the logic later.
        # May also need to get the object of data_schema_name and add new information about the tables written into the SkyhookDM

        id_array = range(len(table.columns[0]))
        event_id_col = pa.field('EVENT_ID', pa.int64())

        object_names = []

        # Create a table for each column
        for i in range(len(table.columns)):
            field = table.field(0)
            schema = pa.schema([event_id_col, table.field(i)])
            sub_table = pa.Table.from_arrays([id_array, table.columns[i]],schema = schema)
            
            # Serialize arrow table to bytes
            batches = sub_table.to_batches()
            sink = pa.BufferOutputStream()
            writer = pa.RecordBatchStreamWriter(sink, sub_table.schema)

            for batch in batches:
                writer.write_batch(batch)
            
            buff = sink.getvalue()
            buff_bytes = buff.to_pybytes()

            sub_table_name = data_schema_name + '#' + table_name + '#' + table.column_names[i] + '.0.0'
            fu = self.client.submit(runOnDriver, buff_bytes, sub_table_name, self.ceph_pool)
            result = fu.result()
            if result is True:
                object_names.append(sub_table_name)
        
        return object_names


    def addDatasetSchema(self, schema_json, data_type = 'servicex'):
        def runOnDriver(schema_json, name, data_type, ceph_pool):
            from skyhookdmdriver import skyhook_driver as sd
            res = sd.addDatasetSchema(schema_json, name, data_type, ceph_pool)
            return res

        values = json.loads(schema_json)

        # the name of the schema is named after the did for now
        name = values['did']

        fu = self.client.submit(runOnDriver, schema_json, name, data_type, self.ceph_pool)
        result = fu.result()





    def writeDataset(self, path, dstname):
        def runOnDriver(path, dstname, poolname, addr ):
            # import skyhook_driver as sd
            from skyhookdmdriver import skyhook_driver as sd
            res = sd.writeDataset(path, dstname, addr, poolname)
            return res

        fu = self.client.submit(runOnDriver, path, dstname, self.ceph_pool, self.addr)
        result = fu.result()
        return result

    def getDataset(self, name):
        def runOnDriver(name, ceph_pool):
            from skyhookdmdriver import skyhook_driver as sd
            res = sd.getDataset(name, ceph_pool)
            return res

        fu = self.client.submit(runOnDriver, name, self.ceph_pool)
        result = fu.result()
        
        data = json.loads(result)
        files = [] 
        for item in data['files']:
            file = File(item['name'], item['file_attributes'], item['file_schema'], name, str(item['ROOTDirectory']))
            files.append(file)

        dataset = Dataset(data['dataset_name'], data['size'], files)
        return dataset

    def runQuery(self, obj, querystr):
        #limit just to 1 obj
        # command_template = '--wthreads 1 --qdepth 120 --query hep --pool hepdatapool --start-obj #startobj --output-format \"SFT_PYARROW_BINARY\" --data-schema \"#dataschema\" --project \"#colname\" --num-objs #objnum --oid-prefix \"#prefix\" --table-name \"#tablename\" --subpartitions 10'
        command_template = ['--wthreads', '1',  '--qdepth', '120',  '--query', 'hep', '--pool', self.ceph_pool, '--output-format', 'SFT_PYARROW_BINARY', '--num-objs', '1', '--subpartitions', '10']


        def generateQueryCommand(file, querystr):
            cmds = []
            prefix = file.dataset + '#' + str(file.name) + '#' + str(file.ROOTDirectory)
            brs = querystr.split('project')[-1].split(',')
            obj_num = 0

            for br in brs:
                br = br.strip()
                elems = br.split('.')
                br_name = elems[-1]  
                elems.remove(br_name)
                local_prefix = ''
                for elem in elems:
                    local_prefix = local_prefix + '#' + elem.strip()
                obj_prefix = prefix + local_prefix + '#'
                data_schema = ''

                # print(obj_prefix)

                f_schema = file.getSchema()
                found = False

                for i in range(len(elems)):
                    for j in range(len(f_schema['children'])):
                        ch_sche = f_schema['children'][j]
                        if elems[i].strip() == ch_sche['name']:
                            f_schema = ch_sche
                            break

                obj_num = len(f_schema['children'])

                for m in range(len(f_schema['children'])):
                    ch_sche = f_schema['children'][m]
                    if br_name.strip() == ch_sche['name']:
                        found = True
                        f_schema = ch_sche
                        #limit the obj num to 1
                        obj_num = m
                        break

                if found:
                    cmd = command_template.copy()
                    cmd.append('--start-obj')
                    cmd.append(str(obj_num))

                    data_schema = '0 4 0 0 EVENT_ID;' + f_schema['data_schema'] + ';'
                    cmd.append('--data-schema')
                    cmd.append(data_schema)

                    cmd.append('--project')
                    cmd.append('event_id,'+ br_name.strip())

                    cmd.append('--oid-prefix')
                    cmd.append(obj_prefix)
                    
                    cmd.append('--table-name')
                    cmd.append(br_name.strip())
                    
                    #limit the obj num to 1
                    cmds.append(cmd)
            return cmds



        def exeQuery(command):
            import subprocess

            prog = '/mnt/sdb/skyhookdm-ceph/build/bin/run-query'
            cmd = []
            cmd.append(prog)
            cmd.extend(command)
            
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            result = p.communicate()[0]

            return result


        def _mergeTables(tables):
            bigtable = None
            for table in tables:
                if bigtable == None:
                    bigtable = table.remove_column(0)
                else:
                    bigtable = bigtable.append_column(table.field(1), table.columns[1])

            return bigtable.replace_schema_metadata(None)


        def fileQuery(obj, querystr):
            cmds = generateQueryCommand(obj, querystr)
            futures = []
            for command in cmds:
                future = self.client.submit(exeQuery, command)
                futures.append(future)

            tablestreams = self.client.gather(futures)
            tables = []

            for tablestream in tablestreams:
                sizebf = None
                stream_length = len(tablestream)
                cursor = 0
                batches = []

                while True:
                    if cursor == stream_length:
                        break
                    sizebf = tablestream[cursor:cursor+4]
                    cursor = cursor + 4
                    size = struct.unpack("<i",sizebf)[0]
                    stream = tablestream[cursor:cursor+size]
                    cursor = cursor + size
                    reader = pa.ipc.open_stream(stream)
                    for b in reader:
                        batches.append(b)

                #sort batches
                def mykey(batch):
                    tb = pa.Table.from_batches([batch.slice(0,1)])
                    return tb.columns[0][0].as_py()

                batches = sorted(batches,key=mykey)

                table = pa.Table.from_batches(batches)
                tables.append(table)

            res = _mergeTables(tables)

            res = LazyDataframe(res)
            return res


        if 'File' in str(obj):
            res = fileQuery(obj, querystr)
            return res

        if 'Dataset' in str(obj):

            rtfiles = obj.getFiles()
            global_tables = []
            futures_set = []

            for rtfile in rtfiles:
                cmds = generateQueryCommand(rtfile, querystr)
                futures = []
                for command in cmds:
                    future = self.client.submit(exeQuery, command)
                    futures.append(future)
                futures_set.append(futures)

            for futures in futures_set:

                tablestreams = self.client.gather(futures)
                tables = []

                for tablestream in tablestreams:
                    sizebf = None
                    stream_length = len(tablestream)
                    cursor = 0
                    batches = []

                    while True:
                        if cursor == stream_length:
                            break
                        sizebf = tablestream[cursor:cursor+4]
                        cursor = cursor + 4
                        size = struct.unpack("<i",sizebf)[0]
                        stream = tablestream[cursor:cursor+size]
                        cursor = cursor + size
                        reader = pa.ipc.open_stream(stream)
                        for b in reader:
                            batches.append(b)

                    #sort batches
                    def mykey(batch):
                        tb = pa.Table.from_batches([batch.slice(0,1)])
                        return tb.columns[0][0].as_py()

                    batches = sorted(batches,key=mykey)

                    table = pa.Table.from_batches(batches)
                    tables.append(table)

                res = _mergeTables(tables)
                res = LazyDataframe(res)
                global_tables.append(res)

            return global_tables

        return None



class LazyDataframe:

    def __init__(self, arr_table, entrysteps = -1):
        self._arr_table = arr_table
        self._entrysteps = entrysteps
        self._batches = None
        self._current_index = 0

    def __iter__(self):
        return self


    def __next__(self):
        if self._batches is None:
            raise Exception('Please set entrysteps using set_entrysteps() function before use!') 

        if self._current_index >= len(self._batches): 
            self._current_index = 0
            raise StopIteration

        sub_table = pa.Table.from_batches(self._batches[self._current_index:self._current_index + 1])
        self._current_index = self._current_index + 1

        chunk = sub_table.to_pydict()

        for item in chunk:
            chunk[item] = awkward.fromiter(chunk[item])

        return chunk

    def set_entrysteps(self, entrysteps):
        if self._batches is not None:
            self._arr_table = pa.Table.from_batches(self._batches)

        self._entrysteps = entrysteps
        self._batches = self._arr_table.to_batches(entrysteps)


