# Import the SkyhookDM library
from skyhookdmpy import SkyhookDM

# Create a SkyhookDM() object
sk = SkyhookDM()

# Connect to the Skyhook Driver given the ip_address. 
sk.connect('ip_address', 'hepdatapool')

# Write the dataset to Ceph. As the following data is already loaded into ceph. I commented them out for now. 
# urls = ['./Run2012B_DoubleMuParked.root','./Run2012C_DoubleMuParked.root']
# sk.writeDataset(urls,'demodst')

# Get the dataset
dst = sk.getDataset('demodst')

# Run queries which return a number of tables according to the number of the files included in the dataset. 
# tables = sk.runQuery(dst,'select *, project Events;1.nMuon, Events;1.Muon_pt, Events;1.Muon_eta, Events;1.Muon_mass, Events;1.Muon_charge')
tables = sk.runQuery(dst,'select *, project Events;1.nMuon, Events;1.Muon_pt')

# Convert the second table to pandas dataframe. It may take a few secs. 
dataframe = tables[1].to_pandas()

# Print the data frame.
print(dataframe)
