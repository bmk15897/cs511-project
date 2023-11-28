# CS 511 Project
## Kafka vs. Redpanda Comparison

This Project conducts a targeted comparison between Apache Kafka and Redpanda, two prominent distributed streaming platforms. We shift our focus from general features to specific performance metrics, primarily examining producer and consumer latencies. Our analysis systematically alters key variables: the number of producers and consumers, the number of brokers in the cluster (either one or three), and the message load per second. Through this approach, we present a nuanced latency comparison under varying conditions. Notably, we observe that Redpanda's often-cited performance advantages \cite{redpandakafkabench}, especially in high-throughput scenarios exceeding 1Gb/s, might not translate effectively to smaller-scale operations. We developed adaptable scripts for effortless benchmarking, which can be tailored to specific operational needs. Our results are presented in easy-to-understand graphical formats, aiding in clear and informed decision-making.