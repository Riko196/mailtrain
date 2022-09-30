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

set -e

if [[ -z $SLURM_JOB_ID ]]; then
    echo "not running under Slurm" 1>&2
    exit 1
fi

ch-image pull mariadb:10.6.7
ch-convert mariadb:10.6.7 ~/mariadb_container
ch-image delete mariadb:10.6.7

ch-image pull mongo:4.2.22
ch-convert mongo:4.2.22 ~/mongo_container
ch-image delete mongo:4.2.22

ch-image pull haproxy
ch-convert haproxy ~/haproxy_container
ch-image delete haproxy

mkdir mailtrain_container && cd mailtrain_container
ch-image build ./ -f ../Dockerfile --force
cd .. && rm -rf mailtrain_container
ch-convert mailtrain_container ~/mailtrain_container
ch-image delete mailtrain_container
