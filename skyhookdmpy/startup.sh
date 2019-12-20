pkill screen
sleep 1
screen -d -m dask-scheduler --host localhost
screen -d -m dask-worker localhost:8786 --nthreads 20
ps -C dask-scheduler >/dev/null && echo "dask-scheduler is running." || echo "ERROR:dask-scheduler is not running!"
ps -C dask-worker >/dev/null && echo "dask-worker is running." || echo "ERROR:dask-worker is not running!"
echo "Done!"
