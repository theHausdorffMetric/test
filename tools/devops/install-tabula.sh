#! /usr/bin/env bash

#NOTE shall we install or check java?

# unofficial strict mode
set -eo pipefail

readonly PATH_TARGET="/usr/local/bin"

_download_tabula() {
  local tabula_version=$1

  java -version

  echo "downloading tabula jar version ${tabula_version}"

  # as copied from https://github.com/Kpler/kp-containers/blob/master/library/kp-scrapers-pdf/Dockerfile
  curl -Lk
    -D curl-log --fail -L \
    "https://github.com/tabulapdf/tabula-java/releases/download/v${tabula_version}/tabula-${tabula_version}-jar-with-dependencies.jar" \
    > "${PATH_TARGET}/tabula.jar"
}

_download_tabula "1.0.2"
