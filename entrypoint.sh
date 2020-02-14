#!/bin/bash
dask-scheduler --host localhost &
dask-worker localhost:8786 --nthreads 20 &
