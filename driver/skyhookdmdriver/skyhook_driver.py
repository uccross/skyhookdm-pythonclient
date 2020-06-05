#This is an example of dask server side skyhook_driver library.
from skyhookdmdriver.skyhook_common import *
import flatbuffers
import rados

def addDatasetSchema(schema_json, name, data_type, ceph_pool):
    try: 
        cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        cluster.connect()
        ioctx = cluster.open_ioctx(ceph_pool)
        ioctx.aio_write_full(name, bytes(schema_json,'utf-8'))
        ioctx.set_xattr(name, 'size', bytes(str(len(schema_json)),'utf-8'))
        ioctx.set_xattr(name, 'data_type', bytes(data_type,'utf-8'))
        ioctx.close()
        cluster.shutdown()

    except Exception as e:
        return str(e)

    return True


def writeToCeph(buff_bytes, name,  ceph_pool):
    # Write to the Ceph cluster
    try: 
        cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        cluster.connect()
        ioctx = cluster.open_ioctx(ceph_pool)
        ioctx.aio_write_full(name, buff_bytes)
        ioctx.set_xattr(name, 'size', bytes(str(len(buff_bytes)),'utf-8'))
        ioctx.close()
        cluster.shutdown()

    except Exception as e:
        return str(e)

    return True

def readFromCeph(name, ceph_pool):
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(ceph_pool)
    size = ioctx.get_xattr(name, "size")
    data = ioctx.read(name, length = int(size))
    ioctx.close()
    cluster.shutdown()
    return data


def getDataset(dstname, ceph_pool):
    try:
        data = readFromCeph(dstname, ceph_pool)
    except Exception as e:
        return str(e)

    return data


def writeDataset(file_urls, dstname, addr, ceph_pool, dst_type = 'root'):

    result = False

    if dst_type is 'root':
        from skyhookdmdriver import hep
        result = hep.writeRootDataset(file_urls, dstname, addr, ceph_pool)
    
    return result
