from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost')
dst = sk.getDataset('mm')
f = dst.getFiles()[0]
table = sk.runQuery(f,'select event>X, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')
tables = sk.runQuery(dst,'select event>X, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')