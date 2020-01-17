import pyarrow as pa
import pandas as pd

# Setup SkyhookDM
from skyhookdmpy import SkyhookDM

# Create a new SkyhookDM object
sk = SkyhookDM()

# Connect to the skyhook driver given the IP of the SSL env
sk.connect('192.170.236.173')

# Create a panda dataframe with one column named 'a'
df = pd.DataFrame({"a": [1, 2, 3]})

# Convert the panda dataframe to arrow table. 
table = pa.Table.from_pandas(df)

# Write the arrow table to the Ceph cluster.
# This function is under development and it should accept more arguments such as metadata of the table. 
sk.writeArrowTable(table,'tname')

# Query functions of SkyhookDM are not compatible with the data written by using the writeArrowTable() function for now. 
