spin up a cluster of your choice and configure it with DSE 4.7.0

populate tables by running the following cassandra stress commands with the given profile (make sure you have enough space)


ran tests with the following command and configuration options:

    sudo dse spark-submit --driver-memory 2g --executor-memory=2g --class pro.foundev.benchmarks.spark_throughput.OperationThroughputBenchmarkRunner spark_throughput-assembly.jar 10.240.31.25  spark://10.240.26.33:7077
