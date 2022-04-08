#!/bin/bash

############################################################################
#
# Install python dependencies
# Usage:
#   ./scripts/install.sh    : Install dependencies
#   ./scripts/install.sh -u : Upgrade requirements.txt & Install dependencies
#
############################################################################
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$( dirname $SCRIPTS_DIR )"

print_horizontal_line() {
  echo "------------------------------------------------------------"
}

print_status() {
  print_horizontal_line
  echo "--*--> $1"
  print_horizontal_line
}

main() {
  UPDATE=0
  if [[ "$#" -ge 1 ]] && [[ "$1" = "-u" || "$1" = "update" ]]; then
    UPDATE=1
  fi

  print_status "Installing workspace: ${ROOT_DIR}"
  print_status "Installing pip-tools"
  python -m pip install pip-tools

  if [[ $UPDATE -eq 1 ]]; then
    print_status "Compiling requirements"
    cd $ROOT_DIR
    CUSTOM_COMPILE_COMMAND="./scripts/install.sh -u" \
    pip-compile --upgrade --pip-args "--no-cache-dir"
    print_horizontal_line
  fi

  print_status "Installing dependencies"
  pip install -r $ROOT_DIR/requirements.txt --no-deps
  print_horizontal_line

  print_status "Installing workspace $ROOT_DIR"
  pip3 install --no-deps --editable $ROOT_DIR
  print_horizontal_line
}

main "$@"
