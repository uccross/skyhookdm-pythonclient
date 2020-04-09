# This is an example which simulates the use case here: https://github.com/CoffeaTeam/coffea/blob/master/binder/muonspectrum_v1.ipynb

import time
import uproot
import uproot_methods
import awkward
from skyhookdmclient import SkyhookDM
import numpy
from coffea import hist

sk = SkyhookDM()
sk.connect('192.170.236.173','hepdatapool')
dst = sk.getDataset('nanoexample')

tstart = time.time()
masshist = hist.Hist("Counts", hist.Bin("mass", r"$m_{\mu\mu}$ [GeV]", 30000, 0.25, 300))

tables = sk.runQuery(dst,'select *, project Events;1.nMuon,Events;1.Muon_pt,Events;1.Muon_eta,Events;1.Muon_phi,Events;1.Muon_mass,Events;1.Muon_charge')
table = tables[0]

table.set_entrysteps(2)

for chunk in table:
    p4 = uproot_methods.TLorentzVectorArray.from_ptetaphim(
        chunk.pop('MUON_PT'),
        chunk.pop('MUON_ETA'),
        chunk.pop('MUON_PHI'),
        chunk.pop('MUON_MASS'),
    )
    muons = awkward.JaggedArray.zip(p4=p4, charge=chunk['MUON_CHARGE'])

    twomuons = (muons.counts == 2)
    opposite_charge = (muons['charge'].prod() == -1)
    dimuons = muons[twomuons & opposite_charge].distincts()
    dimuon_mass = (dimuons.i0['p4'] + dimuons.i1['p4']).mass
    masshist.fill(mass=dimuon_mass.flatten())
    
elapsed = time.time() - tstart
# fig, ax, _ = hist.plot1d(masshist)
# ax.set_xscale('log')
# ax.set_yscale('log')
# ax.set_ylim(0.1, 1e6)
print("Events/s:", masshist.values()[()].sum()/elapsed)
