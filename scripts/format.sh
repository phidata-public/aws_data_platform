#!/bin/bash

############################################################################
#
# Format workspace using black and mypy
# Usage:
#   ./scripts/format.sh
#
############################################################################

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$( dirname ${SCRIPTS_DIR} )"

print_horizontal_line() {
  echo "------------------------------------------------------------"
}

print_status() {
  print_horizontal_line
  echo "--*--> $1"
  print_horizontal_line
}

main() {
  print_status "Running: black ${ROOT_DIR}"
  black ${ROOT_DIR}
  print_status "Running: mypy ${ROOT_DIR} --config-file ${ROOT_DIR}/pyproject.toml"
  mypy ${ROOT_DIR} --config-file ${ROOT_DIR}/pyproject.toml
}

main "$@"
