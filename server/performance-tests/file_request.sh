#!/bin/bash

# URL for file request (HA)PUBLIC server and REST API argument (set your value)
file_url=http://localhost:8001/files/campaign/file/1/6e49502594b81c1e1397d565747f1af9
#file_url=http://parlab.ms.mff.cuni.cz:7020/files/campaign/file/1/6e49502594b81c1e1397d565747f1af9

# File requests count
request_count=2000

# Execute ${request_count} parallel file requests and store them in /dev/null (deleting)
seq 1 ${request_count} | xargs -I{} -n 1 -P 25 curl "${file_url}" -o "/dev/null"
