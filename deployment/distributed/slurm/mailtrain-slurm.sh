#!/bin/bash
## #SBATCH --time=0:30:00 -N 6 -c 16 -p mpi-homo-short --mem 101G
#
# Login into the cluster:
#
#   $ ssh kucake@parlab.ms.mff.cuni.cz -p 42222
#
# Build image (get Dockerfile from mailtrain/deployment/distributed and run commands):
#
#   1. ch-image build ./mailtrain --force .
#   2. ch-convert mailtrain ~/mailtrain
#
# Run an example mailtrain sender-workers computation. Requires four arguments:
#
#   1. Image directory
#   2. High-speed network interface name
#   3. R/W directory
#   4. Application
# 
# Tunnel port:
#
#   $ ssh -L :7020:127.0.0.1:22 ${server_ip}
#
# Run app in Slurm cluster from home directory '~':
#
#   $ sbatch -p mpi-homo-short -A kdsstudent mailtrain-slurm.sh ~/mailtrain eno1 ~/mariadb ~/mongodb ~/tmp
#

set -e

if [[ -z $SLURM_JOB_ID ]]; then
    echo "not running under Slurm" 1>&2
    exit 1
fi

img=$1
dev=$2
mariadbdir=$3
mongodbdir=$4
tmpdir=$5

# What IP address to use for mariadb and mongodb server?
if [[ -z $dev ]]; then
    echo "no high-speed network device specified"
    exit 1
fi
server_ip=$(  ip -o -f inet addr show dev "$dev" \
            | sed -r 's/^.+inet ([0-9.]+).+/\1/')
mongodb_url=mongodb://${server_ip}:27017
if [[ -n $server_ip ]]; then
    echo "MariaDB and MongoDB server IP: ${server_ip}"
else
    echo "no IP address for ${dev} found"
    exit 1
fi

# Start the mariadb server
ch-run -b "$mariadbdir:/var/lib/mysql" -b "$tmpdir/mysqld:/run" "$img" -- /etc/init.d/mysql start &
sleep 5

echo "MariaDB server is running!"

# Start the mongodb server and initialize mongodb cluster
ch-run -b "$mongodbdir:/data/db" -b "$tmpdir:/tmp" "$img" -- mongod &
sleep 5
ch-run -b "$mongodbdir:/data/db" -b "$tmpdir:/tmp" "$img" -- "mongosh --eval 'rs.initiate()'"

echo "MongoDB server is running!"

# Start HAProxy
ch-run "$img" -- haproxy
sleep 3

echo "HAProxy is running!"

# Start sender-workers
mailtrain_src="${img}/opt/mailtrain/server"
srun -p mpi-homo-short -A kdsstudent sh -c "(SLURM_MONGODB_URL='${mongodb_url}' ch-run -c '${mailtrain_src}' '${img}' -- \
                       node services/sender-worker.js \
                       && sleep infinity)"

# Let Slurm kill the workers and server
