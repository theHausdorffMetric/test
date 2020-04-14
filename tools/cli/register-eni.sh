#! /usr/bin/env bash

# Some european vesselshave ENI and no IMO, hence usual process doesn't work.
# This script takes a list of mmsi and register that instead.

# unofficial strict mode
set -eo pipefail
IFS=$'\n\t'

BASE_URL="https://services.marinetraffic.com/api/setfleet"
SET_KEY="ea1a65ef4b5f18a61645bba06a6e267f5820f234"


register_on_mt() {
  local data_file="$1"

  cut -d" " -f2 "$data_file" | while read -r mmsi
  do
    printf "\nregistering MMSI %s\n" "$mmsi"

    curl \
      -H 'Accept: application/json' \
      "$BASE_URL/$SET_KEY/mmsi:${mmsi}/fleet_id:927308/active:1/protocol:jsono"
  done
}

register_on_mt "$1"
