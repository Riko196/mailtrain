#!/bin/bash
#SBATCH --time=2:00:00 --nodes=8 -n 256 -p mpi-homo-short -A kdsstudent -c 2 --mem-per-cpu=1G
#
# Login into the cluster:
#
#   $ ssh kucake@parlab.ms.mff.cuni.cz -p 42222
#
# Run script in Slurm cluster from home directory '~':
#
#   $ sbatch -p mpi-homo-short -A kdsstudent create-containers.sh
#
# Run Mailtrain app from home directory '~', it requires four arguments:
#
#   1. High-speed network interface name
#   2. Mariadb database directory
#   3. MongoDB database directory
#   4. TMP directory
# 
# Example command for running app in Slurm cluster from home directory '~':
#
#   $ sbatch -p mpi-homo-short -A kdsstudent mailtrain-slurm.sh eno1 ~/mariadb ~/mongodb ~/tmp
#
# Tunnel port:
#
#   $ ssh -L :7020:127.0.0.1:3000 ${mailtrain_ip}
#
# MariaDB access: 
#   $ ch-run -b "./mariadb:/var/lib/mysql" -b "./tmp/mariadb:/run" "./mariadb_container/" -- mysql -u mailtrain -p
#

set -e

if [[ -z $SLURM_JOB_ID ]]; then
    echo "not running under Slurm" 1>&2
    exit 1
fi

mailtrainimg=~/mailtrain_container
mariadbimg=~/mariadb_container
mongodbimg=~/mongo_container
haproxyimg=~/haproxy_container
dev=$1
mariadbdir=$2
mongodbdir=$3
tmpdir=$4

# What IP address to use for mariadb and mongodb server?
if [[ -z $dev ]]; then
    echo "no high-speed network device specified"
    exit 1
fi
mailtrain_ip=$(  ip -o -f inet addr show dev "$dev" \
            | sed -r 's/^.+inet ([0-9.]+).+/\1/')
if [[ -n $mailtrain_ip ]]; then
    echo "Mailtrain main node IP: ${mailtrain_ip}"
else
    echo "no IP address for ${dev} found"
    exit 1
fi

# Start the mariadb server
ch-run -b "$mariadbdir:/var/lib/mysql" -b "$tmpdir/mariadb:/run" "$mariadbimg" -- /usr/local/bin/docker-entrypoint.sh --verbose --bind-address=localhost &
sleep 2

echo "MariaDB server is running!"

# Start and initialize MongoDB cluster
ch-run -b "$mongodbdir:/data/db" -b "$tmpdir/mongodb:/tmp" "$mongodbimg" -- /usr/local/bin/docker-entrypoint.sh --config /etc/mongod.conf.orig &
sleep 10
ch-run -b "$mongodbdir:/data/db" -b "$tmpdir/mongodb:/tmp" "$mongodbimg" -- mongo --eval "rs.initiate()" 
ch-run -b "$mongodbdir:/data/db" -b "$tmpdir/mongodb:/tmp" "$mongodbimg" -- mongo --eval "rs.status()"

echo "MongoDB cluster is running!"

# Set mailtrain_src path and MongoDB URL
mailtrain_src="/opt/mailtrain/server"
mongodb_url=mongodb://${mailtrain_ip}:27017
# Start HAProxy
# ch-run -b "$tmpdir/haproxy:/var/run" "$haproxyimg" -- /usr/local/bin/docker-entrypoint.sh haproxy -f /usr/local/etc/haproxy/haproxy.cfg &
# sleep 5
# echo "HAProxy is running!"
# Start sender-workers
# hapublic_worker_port=3005
# srun -p mpi-homo-short -A kdsstudent -n 8 sh -c "(SLURM_MONGODB_URL='${mongodb_url}' HAPUBLIC_WORKER_PORT=${hapublic_worker_port} ch-run -c '${mailtrain_src}' -b '$tmpdir/files:/opt/mailtrain/server/# files' '${mailtrainimg}' -- \
                      #node lib/hapublic/hapublic-worker.js \
                      #&& sleep infinity)"

# Start Mailtrain
ch-run -c "$mailtrain_src" -b "$tmpdir/files:/opt/mailtrain/server/files" "$mailtrainimg" -- node index.js

# Start sender-workers
srun -p mpi-homo-short -A kdsstudent -n 256 sh -c "(SLURM_MONGODB_URL='${mongodb_url}' ch-run -c '${mailtrain_src}' '${mailtrainimg}' -- \
                      node services/sender-worker.js \
                      && sleep infinity)"

echo "Mailtrain main node IP: ${mailtrain_ip}"
