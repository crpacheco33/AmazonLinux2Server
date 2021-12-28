#!/bin/bash
while getopts j:a:e:i:r: flag
do
    case "${flag}" in
        j) JOB=${OPTARG};;
        a) ACTION=${OPTARG};;
        e) ENTITY=${OPTARG};;
        i) ID=${OPTARG};;
        r) REGION=${OPTARG};;
    esac
done

/usr/bin/make -f "/home/centos/visibly/product/server/server/make/${JOB}" "${ACTION}" -- region="${REGION}" entity_id="${ENTITY}" advertiser_id="${ID}"
# /usr/bin/make -f "/var/www/server/server/make/${JOB}" "${ACTION}" -- region="${REGION}" entity_id="${ENTITY}" advertiser_id="${ID}"
