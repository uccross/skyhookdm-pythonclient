sudo apt-get install python-pip -y
sudo apt-get install jupyter-core -y
sudo apt-get install jupyter-notebook -y
python2 -m pip install ipykernel
pip install "dask[complete]"
pip install pyarrow
pip install uproot
pip install bokeh
pip install backports.lzma
sudo cp * /usr/lib/python2.7/
pip install wget
rados mkpool hepdatapool