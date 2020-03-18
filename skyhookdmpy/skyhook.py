import struct
from skyhookdmpy.skyhook_common import *

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
    def writeArrowTable(self, table, name):

        def runOnDriver(buff_bytes, name,  ceph_pool):
            from skyhookdmpy import skyhook_driver as sd
            res = sd.writeArrowTable(buff_bytes, name, ceph_pool)
            return res

        #Serialize arrow table to bytes
        batches = table.to_batches()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, table.schema)

        for batch in batches:
            writer.write_batch(batch)
        buff = sink.getvalue()
        buff_bytes = buff.to_pybytes()

        fu = self.client.submit(runOnDriver, buff_bytes, name, self.ceph_pool)
        result = fu.result()

        return result


    def writeDataset(self, path, dstname):
        def runOnDriver(path, dstname, poolname, addr ):
            # import skyhook_driver as sd
            from skyhookdmpy import skyhook_driver as sd
            res = sd.writeDataset(path, dstname, addr, poolname)
            return res

        fu = self.client.submit(runOnDriver, path, dstname, self.ceph_pool, self.addr)
        result = fu.result()
        return result

    def getDataset(self, name):
        def runOnDriver(name, ceph_pool):
            from skyhookdmpy import skyhook_driver as sd
            res = sd.getDataset(name, ceph_pool)
            return res

        fu = self.client.submit(runOnDriver, name, self.ceph_pool)
        result = fu.result()
        
        data = json.loads(result)
        files = []
        for item in data['files']:
            file = File(item['name'], item['file_attributes'], item['file_schema'], name, item['ROOTDirectory'])
            files.append(file)

        dataset = Dataset(data['dataset_name'], data['size'], files)
        return dataset

    def runQuery(self, obj, querystr):
        #limit just to 1 obj
        command_template = '--wthreads 1 --qdepth 120 --query hep --pool {} --start-obj #startobj --output-format \"SFT_PYARROW_BINARY\" --data-schema \"#dataschema\" --project \"#colname\" --num-objs #objnum --oid-prefix \"#prefix\" --table-name \"#tablename\" --subpartitions 10'.format(self.ceph_pool)


        def generateQueryCommand(file, querystr):
            cmds = []
            prefix = file.dataset + '#' + file.name + '#' +file.ROOTDirectory
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
                    cmd = command_template
                    data_schema = '0 4 0 0 EVENT_ID;' + f_schema['data_schema'] + ';'
                    cmd = cmd.replace('#dataschema', data_schema)
                    cmd = cmd.replace('#colname', 'event_id,'+ br_name.strip())
                    cmd = cmd.replace('#prefix', obj_prefix)
                    cmd = cmd.replace('#tablename', br_name.strip())
                    #limit the obj num to 1
                    cmd = cmd.replace('#objnum', str(1))
                    cmd = cmd.replace('#startobj', str(obj_num))
                    cmds.append(cmd)
            return cmds



        def exeQuery(command):
            prog = '/mnt/sdb/skyhookdm-ceph/build/bin/run-query '
            import os
            result = os.popen(prog + command).read()
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
                global_tables.append(res)

            return global_tables

        return None







