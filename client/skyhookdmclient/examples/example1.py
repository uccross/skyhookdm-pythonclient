from skyhookdmclient import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost', 'hepdatapool')
dst = sk.getDataset('aod')
f = dst.getFiles()[0]
table = sk.runQuery(f,'select *, project Events;75.Muon_phi')
tables = sk.runQuery(dst,'select *, project Events;75.Muon_eta,Events;75.Muon_phi,Events;75.Muon_mass')
