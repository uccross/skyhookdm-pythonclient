from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost')
urls = ['https://github.com/uccross/skyhookdm-pythonclient/blob/master/nano_aod.root?raw=true']
sk.writeDataset(urls,'nanoexample')
dst = sk.getDataset('nanoexample')
files = dst.getFiles()
file = files[0]
schema = file.getSchema()
rt = file.getRoot()
children = rt.getChildren()
file.getAttributes()
table = sk.runQuery(file,'select event>X, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')