#!/usr/bin/env bash
# Unsigned download of Argoverse 2 Sensor val logs. Never touches the test split.
set -euo pipefail
list=${1:?usage: fetch_av2.sh <file-with-log-ids>}
dest=${DP_DATA:-$HOME/data/av2}/sensor/val
mkdir -p "$dest"
while read -r id; do
  [ -z "$id" ] && continue
  aws s3 sync --no-sign-request --only-show-errors "s3://argoverse/datasets/av2/sensor/val/$id/" "$dest/$id/"
  echo "fetched $id: $(find "$dest/$id" -type f | wc -l) files"
done < "$list"
