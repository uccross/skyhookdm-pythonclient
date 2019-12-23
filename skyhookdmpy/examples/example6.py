from skyhookdmpy import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.150')
urls = ['/mnt/sda4/Run2012B_DoubleMuParked.root','/mnt/sda4/Run2012C_DoubleMuParked.root']
sk.writeDataset(urls,'demodst')
dst = sk.getDataset('exampledst')
tables = sk.runQuery(dst,'select *, project Events;1.nMuon, Events;1.Muon_pt, Events;1.Muon_eta, Events;1.Muon_mass, Events;1.Muon_charge')
dataframe = tables[0].to_pandas()
print(dataframe)