import rados
import sys
from skyhookdmpy import SkyhookDM

if len(sys.argv) > 1:
    dask_server = sys.argv[1]
else:
    dask_server = 'localhost'

# create pool
ceph_pool = 'hepdatapool'
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
if ceph_pool not in cluster.list_pools():
    cluster.create_pool(ceph_pool)

sk = SkyhookDM()
sk.connect(dask_server, ceph_pool)
urls = ['https://github.com/uccross/skyhookdm-pythonclient/raw/master/skyhookdmpy/rsc/nano_aod.root']
sk.writeDataset(urls,'nanoexample')
dst = sk.getDataset('nanoexample')
files = dst.getFiles()
file = files[0]
schema = file.getSchema()
rt = file.getRoot()
children = rt.getChildren()
file.getAttributes()
table = sk.runQuery(file,'select *, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')
tables = sk.runQuery(dst,'select *, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')
sk.writeArrowTable(table,'mytable')
