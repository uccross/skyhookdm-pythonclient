from skyhookdmclient import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost', 'hepdatapool')
urls = ['http://opendata.cern.ch/record/12352/files/VBF_HToTauTau.root']
sk.writeDataset(urls,'nanodst')
dst = sk.getDataset('nanodst')
files = dst.getFiles()
file = files[0]
rootnode = file.getRoot()
trees = rootnode.getChildren()
tree = trees[0]
branches = tree.getChildren()
table = sk.runQuery(file,'select *, project Events;1.nMuon, Events;1.Muon_pt, Events;1.Muon_eta, Events;1.Muon_mass, Events;1.Muon_charge')
dataframe = table.to_pandas()
print(dataframe)
