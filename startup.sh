pkill screen
sleep 1
ip_addr=$(ifconfig eno1 2>/dev/null|awk '/inet / {print $2}'|sed 's/addr://');
screen -d -m dask-scheduler --host $ip_addr
screen -d -m dask-worker $ip_addr:8786 --nthreads 20
ps -C dask-scheduler >/dev/null && echo "dask-scheduler is running." || echo "ERROR:dask-scheduler is not running!"
ps -C dask-worker >/dev/null && echo "dask-worker is running." || echo "ERROR:dask-worker is not running!"
echo "Done!"
