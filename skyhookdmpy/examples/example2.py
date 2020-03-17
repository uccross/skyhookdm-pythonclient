from skyhookdmpy import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost', 'hepdatapool')
dst = sk.getDataset('mm')
f = dst.getFiles()[0]
table = sk.runQuery(f,'select *, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')
tables = sk.runQuery(dst,'select *, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.FatJet_n3b1')
