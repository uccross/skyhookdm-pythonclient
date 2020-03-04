**Quick Start** (suppose that you have the Skyhook Cloudlab expeirment running):

* Run the script of "[setup.sh](https://github.com/uccross/skyhookdm-pythonclient/blob/master/setup.sh)" to check and install dependencies and create the required ceph pool.
* Logout the ssh session and login again. 
* Run the script of "[startup.sh](https://github.com/uccross/skyhookdm-pythonclient/blob/master/startup.sh)" and you're ready to go.

Note: Cloning this repo is not necessary. Runing the scripts is enough. The scripts use 'pip' to install skyhookdmpy and other dependencies which can't be installed via 'pip' commands. 

</br>
</br>

**========================= More Details about SkyhookDM Python APIs =========================**

</br>
</br>

**Skyhook_common:**

This module defines the classes such as Dataset, File, RootNode  which should be understood by both the modules of SkyhookDM and Skyhook_driver.  

_Classes_:

<p align="center"><a href="https://github.com/uccross/skyhookdm-pythonclient"><img src="https://github.com/uccross/skyhookdm-pythonclient/raw/master/skyhookdmpy/rsc/Classes_in_Skyhook_common.png" width="45%"></a></p>

* Dataset:

    * getFiles(): returns a list of File objects in this dataset.

    * getSize(): returns the total size of the Dataset.

* File:

    * getAttributes(): returns the attributes of the file, such as access_time, size, and name.

    * getRoot(): returns the highest level of the RootNode. 

    * getSchema(): returns a python dictionary which describes the logical layout of the file, such as TTree, TBranch, RootDirectory.

* RootNode:

    * getName(): returns the name of the RootNode

    * getClassType(): returns the classtype of this RootNode, such as TTree, TBranch.

    * getDataType(): returns the datatype if the RootNode itself is a TBranch. Otherwise, it returns None.

    * getChildren(): returns the children nodes as a list. 

    * getParent(): returns the parent of the current RootNode.
    

**Skyhook_driver:**

Skyhook_driver handles the heavy workloads such as writing the dataset to the ceph cluster and partition the root files to smaller Arrow tables. Some complicated queries can also be handled by Skyhook_driver. Skyhook_driver can manage multiple workers, schedule tasks and balance the workload between workers. Workers can be added and removed based on the volume of the workload.

**Skyhook:**

For now this module only contains one class which is SkyhookDM. This module and Skyhook_common should be installed by the client. SkyhookDM connects to the Skyhook_driver and submit tasks to Skyhook_driver. Some lightweight queries are also handled by SkyhookDM.

_Classes_:

* SkyhookDM:

    * connect(*ip_addr*): connects Skyhook_driver given its *ip_addr*

    * writeDataset(*urls*, *datasetname*): write the dataset to Ceph data pool. *urls* is a list of urls of Root file contained in the dataset. *datasetname* is the name of the dataset.

    * getDataset(*name*): returns a Dataset object given the dataset name. 

    * runQuery(*obj*, *querystr*): returns an arrow table/tables give the *obj* and *querystr.* The *obj* can be a File object or a Dataset object. The function returns one arrow table if it’s a File object and returns a list of tables if it’s a Dataset object. If multiple branches are queried, an arrow table with multiple columns for each branch is returned.

**Usage Examples:**

==========================
```python
# import the lib

from skyhook import SkyhookDM

# create a new SkyhookDM object

sk = SkyhookDM()

# connect to Skyhook_driver

sk.connect('128.105.144.226')

# give the Root file urls

urls = ['[http://uaf-1.t2.ucsd.edu/jeff_data/files/0C1BA5F0-9253-F24C-BBBA-E78510BC4D8E.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/0C1BA5F0-9253-F24C-BBBA-E78510BC4D8E.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/0E7B346F-89B6-7D4D-8AE0-B1014D09A1BB.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/0E7B346F-89B6-7D4D-8AE0-B1014D09A1BB.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/0FAE126E-B6B9-134D-B732-53EE0A156903.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/0FAE126E-B6B9-134D-B732-53EE0A156903.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/2B00A7C5-908D-3946-9D72-0CB13979BBEF.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/2B00A7C5-908D-3946-9D72-0CB13979BBEF.root)'

,'[http://uaf-1.t2.ucsd.edu/jeff_data/files/8FE19B0A-496C-9740-8600-E8EA265D4859.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/8FE19B0A-496C-9740-8600-E8EA265D4859.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/24C5C749-BC61-1746-8CEE-69D54B33A1D3.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/24C5C749-BC61-1746-8CEE-69D54B33A1D3.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/A8B4FDAE-DEEB-D24E-8DD2-91EB6E2D9CF7.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/A8B4FDAE-DEEB-D24E-8DD2-91EB6E2D9CF7.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/B91E7F75-43E3-C246-B0F8-5C8D2216E299.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/B91E7F75-43E3-C246-B0F8-5C8D2216E299.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/C6E7959A-DB51-BC4C-BCDC-68CD694B919E.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/C6E7959A-DB51-BC4C-BCDC-68CD694B919E.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/E66BCF3F-B328-B244-AC53-788C07E02454.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/E66BCF3F-B328-B244-AC53-788C07E02454.root)',

'[http://uaf-1.t2.ucsd.edu/jeff_data/files/F47D0822-3436-004E-926A-0A294C09AB0D.root](http://uaf-1.t2.ucsd.edu/jeff_data/files/F47D0822-3436-004E-926A-0A294C09AB0D.root)'

]

# write the dataset to Ceph cluster and name the dataset ‘demodst’

sk.writeDataset(urls,'demodst')
```
==========================

```python
# import the lib

from skyhook import SkyhookDM

# create a new SkyhookDM object

sk = SkyhookDM()

# connect to Skyhook_driver

sk.connect('128.105.144.226')

# get the dataset named ‘aod’

dst = sk.getDataset('aod')

# get the first file in the dataset

f = dst.getFiles()[0]

# run the query on the first file, queries only one branch

table = sk.runQuery(f,'select event_id>5, project Events;75.Muon_phi')

# run the query on the dataset, queries multiple branches.

tables = sk.runQuery(dst,'select event_id>5, project Events;75.Muon_eta,Events;75.Muon_phi,Events;75.Muon_mass')
```

## Run in Docker

Assuming we have the following folder structure:

```
myproject/
├── ceph/
│   ├── ceph.client.admin.keyring
│   └── ceph.conf
└── scripts/
    └── example.py
```

where:

  * `scripts/` is where we store scripts we want to execute.
  * `ceph/` contains `ceph.conf` and `ceph.client.admin.keyring` 
    files.

We execute the example by doing the following:

```bash
cd myproject/

docker run --rm -ti \
  -v $PWD:/ws \
  -v $PWD/ceph:/etc/ceph \
  -w /ws \
  uccross/skyhookdm-py \
    /ws/scripts/myapp.py
```

The above mounts the `myproject/` folder in a `/ws` (workspace) folder inside the 
container. It also mounts the `myproject/ceph` folder to `/etc/ceph` 
which is where the skyhookdm-py library expects it. It then invokes 
the `scripts/myapp.py` file that we need to write ourselves. Take a 
look at the [`examples/`](./skyhookdmpy/examples) folder for examples 
of how to write applications that use the Skyhook python client 
library. Example3.py is ready to run. 

## Run in Kubernetes

The `ivotron/skyhookdm-py` container requires Ceph configuration files 
that can be passed as secrets. We first create these two secrets:

```bash
kubectl create secret generic mysecret \
  --from-file=/path/to/ceph.conf \
  --from-file=/path/to/ceph.client.admin.keyring
```

Once the secrets are created, the following executes the test:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mypod
spec:
  containers:
  - name: mypod
    image: ivotron/skyhookdm-py
    args: ['/path/to/script.py']
    volumeMounts:
    - name: ceph-config
      mountPath: "/etc/ceph/"
      readOnly: true
  volumes:
  - name: ceph-config
    secret:
      secretName: mysecret
```

In the above the `/path/to/script.py` needs to be updated to the 
proper script that must already exist inside the image being referenced,
so you could probably build an image on top of the one we provide with 
your scripts. For example:

```Dockerfile
FROM uccross/skyhookdm-py

COPY myscript.py /
```

And reference the above `myscript.py` file in the Pod definition file 
instead of `/path/to/script.py`.
