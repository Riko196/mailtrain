#!/bin/bash
## #SBATCH --time=0:30:00 -N 6 -c 16 -p mpi-homo-short --mem 101G
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
#   $ ssh -L :7020:127.0.0.1:22 ${mailtrain_ip}
#
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
mongodb_url=mongodb://${mailtrain_ip}:27017
if [[ -n $mailtrain_ip ]]; then
    echo "Mailtrain main node IP: ${mailtrain_ip}"
else
    echo "no IP address for ${dev} found"
    exit 1
fi

# Start the mariadb server
ch-run -b "$mariadbdir:/var/lib/mysql" -b "$tmpdir/mariadb:/run" "$mariadbimg" -- /usr/local/bin/docker-entrypoint.sh --verbose --bind-address=localhost &
sleep 5

echo "MariaDB server is running!"

# Start the mongodb server and initialize mongodb cluster
ch-run -b "$mongodbdir:/data/db" -b "$tmpdir/mongodb:/tmp" "$mongodbimg" -- mongod &
sleep 5

echo "MongoDB server is running!"

# Start HAProxy
ch-run -b "$tmpdir/haproxy:/usr/local/etc/haproxy" "$haproxyimg" -- /usr/local/bin/docker-entrypoint.sh &
sleep 3

echo "HAProxy is running!"

# Start Mailtrain
mailtrain_src="${mailtrainimg}/opt/mailtrain/server"
ch-run -c "$mailtrain_src" -b "$tmpdir/files:/opt/mailtrain/server/files" "$mailtrainimg" -- node index.js && sleep infinity
exit 1

# Start sender-workers
mailtrain_src="${mailtrainimg}/opt/mailtrain/server"
srun -p mpi-homo-short -A kdsstudent sh -c "(SLURM_MONGODB_URL='${mongodb_url}' ch-run -c '${mailtrain_src}' '${img}' -- \
                       node services/sender-worker.js \
                       && sleep infinity)"

echo "Mailtrain main node IP: ${mailtrain_ip}"
