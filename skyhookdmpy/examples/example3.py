import os
import rados
import sys
import time

from skyhookdmpy import SkyhookDM

# check if we're connecting to a Dask cluster or spawning our own
if len(sys.argv) > 2:
    raise Exception('Expecting only one parameter')
elif len(sys.argv) == 2:
    dask_server = sys.argv[1]
else:
    dask_server = 'localhost'
    os.system('dask-scheduler --host localhost &')
    os.systme('dask-worker localhost:8786 --nthreads 20 &')
    # wait for Dask to start
    time.sleep(5)
print('Using {} as dask server hostname'.format(dask_server))

# specify the pool that we're operating on
ceph_pool = 'hepdatapool'

# create the pool if it doesn't already exist
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
if ceph_pool not in cluster.list_pools():
    cluster.create_pool(ceph_pool)

# connect to Skyhook instance
sk = SkyhookDM()
sk.connect(dask_server, ceph_pool)

# write data
urls = ['https://github.com/uccross/skyhookdm-pythonclient/raw/master/skyhookdmpy/rsc/nano_aod.root']
sk.writeDataset(urls,'nanoexample')
dst = sk.getDataset('nanoexample')

# read metadata
files = dst.getFiles()
file = files[0]
schema = file.getSchema()
rt = file.getRoot()
children = rt.getChildren()
file.getAttributes()

# read data
table = sk.runQuery(file,'select *, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')
tables = sk.runQuery(dst,'select *, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')

# write data in arrow format
sk.writeArrowTable(table,'mytable')
