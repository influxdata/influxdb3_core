#!/bin/bash
set -e

for directory in *
  do
    if [[ $directory != "target" ]] && [ -d "$directory" -a -d ../influxdb_iox/"$directory" ]; then
        rsync -av --progress --recursive --exclude=trarget ../influxdb_iox/"$directory" .
    fi
  done