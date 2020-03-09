import sys
import pyarrow as pa
import numpy as np
import time
import rados
import FB_Meta as mt
import flatbuffers

def generate_Table(output_file):
    arr = np.random.randint(-sys.maxint,sys.maxint,1320000)
    table = pa.Table.from_arrays([arr],names=['TestColumn'])

    batches = table.to_batches()
    sink = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(sink, table.schema)

    for batch in batches:
        writer.write_batch(batch)

    buff = sink.getvalue()
    buff_bytes = buff.to_pybytes()
    f = open(output_file, 'wb')
    f.write(buff_bytes)

def write_tables(input_file, ceph_pool):
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(ceph_pool)
#     f = open(input_file, 'rb')
#     buf = f.read()

#     reader = pa.ipc.open_stream(buf)
#     table = pa.Table.from_batches(reader)

    start_time = time.time()*1000

    for i in range(100):
        f = open(input_file, 'rb')
        buf = f.read()
        reader = pa.ipc.open_stream(buf)
        table = pa.Table.from_batches(reader)
        
        batches = table.to_batches()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, table.schema)

        for batch in batches:
            writer.write_batch(batch)

        buff = sink.getvalue()
        buff_bytes = buff.to_pybytes()

        ioctx.write_full('table' + str(i), buff_bytes)

    stop_time = time.time()*1000

    ioctx.close()
    cluster.shutdown()

    return stop_time - start_time

def add_FBmeta(databytes):

    builder = flatbuffers.Builder(0)
    datablob = builder.CreateByteVector(databytes)

    mt.FB_MetaStart(builder)
    mt.FB_MetaAddBlobFormat(builder,1)
    mt.FB_MetaAddBlobData(builder,datablob)
    mt.FB_MetaAddBlobSize(builder, len(databytes))
    mt.FB_MetaAddBlobDeleted(builder, False)
    mt.FB_MetaAddBlobOrigOff(builder, 0)
    mt.FB_MetaAddBlobOrigLen(builder, len(databytes))
    mt.FB_MetaAddBlobCompression(builder, 0)
    obj = mt.FB_MetaEnd(builder)
    builder.Finish(obj)
    final_flatbuffer = builder.Output()

    return final_flatbuffer


def write_tables_with_wrapper(input_file, ceph_pool):
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(ceph_pool)
#     f = open(input_file, 'rb')
#     buf = f.read()

#     reader = pa.ipc.open_stream(buf)
#     table = pa.Table.from_batches(reader)    

    start_time = time.time()*1000

    for i in range(100):
        f = open(input_file, 'rb')
        buf = f.read()
        reader = pa.ipc.open_stream(buf)
        table = pa.Table.from_batches(reader) 
        
        sche_meta = {}
        sche_meta['0'] = bytes('0 4 0 0 EVENT_ID;' + str(i) + ' ' + str('integer') + ' 0 1 ' + 'TestColumn')
        schema = table.schema.with_metadata(sche_meta)

        batches = table.to_batches()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, schema)

        for batch in batches:
            writer.write_batch(batch)

        buff = sink.getvalue()
        buff_bytes = buff.to_pybytes()

        obj_with_wrapper = add_FBmeta(buff_bytes)

        ioctx.write_full('table_with_wrapper' + str(i), str(obj_with_wrapper))

    stop_time = time.time()*1000
    ioctx.close()
    cluster.shutdown()

    return stop_time - start_time


output_file = '/users/xweichu/test_table'
generate_Table(output_file)

for i in range(3):
    cost1 = write_tables(output_file,'hepdatapool')
    cost2 = write_tables_with_wrapper(output_file,'hepdatapool')
    print('Time cost without wrapper: '+ str(cost1))
    print('Time cost with wrapper: '+ str(cost2))


