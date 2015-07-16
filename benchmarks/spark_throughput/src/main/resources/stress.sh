#!/bin/sh

#load 10m
cassandra-stress user profile=./stress_10m.yaml n=10000000 ops \(insert=1\)