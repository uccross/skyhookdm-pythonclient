# suppose 
from skyhookdmpy import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.228')
dst = sk.getDataset('nanodst')
tables = sk.runQuery(dst,'select *, project Events;1.nMuon, Events;1.Muon_pt, Events;1.Muon_eta, Events;1.Muon_mass, Events;1.Muon_charge')
dataframe = tables[0].to_pandas()
print(dataframe)
