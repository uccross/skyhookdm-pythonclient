#setup and run skyhookdmclient
sudo docker run -d -ti --name myclient --entrypoint=/bin/bash uccross/skyhookdm-py
sudo docker exec -i -t myclient /bin/bash

#setup and run skyhookdmdriver
sudo docker run -d -ti   --entrypoint=/bin/bash -v $PWD:/ws  -v $PWD/ceph:/etc/ceph  -w /ws  uccross/skyhookdm-driver-py
sudo docker exec -i -t mydriver /bin/bash"