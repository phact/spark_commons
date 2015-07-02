#!/bin/sh
cassandra-stress user profile=./stress.yaml n=10000000 ops\(insert=1\) no-warmup
