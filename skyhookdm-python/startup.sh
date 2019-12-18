pkill screen
sleep 1
screen -d -m dask-scheduler --host localhost
screen -d -m dask-worker localhost:8786 --nthreads 20
echo "SkyhookDM started!"
