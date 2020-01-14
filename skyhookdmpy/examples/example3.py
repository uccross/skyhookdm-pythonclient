from skyhookdmpy import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost')
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
