#!/bin/bash

############################################################################
#
# Install python dependencies
# Usage:
#   ./scripts/install.sh    : Install dependencies
#   ./scripts/install.sh -u : Upgrade + Install dependencies
#
############################################################################
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$( dirname $SCRIPTS_DIR )"

print_horizontal_line() {
  echo "------------------------------------------------------------"
}

print_heading() {
  print_horizontal_line
  echo "--*--> $1"
  print_horizontal_line
}

install_python_deps() {
  print_heading "Installing dependencies"
  pip install -r $ROOT_DIR/requirements.txt --no-deps

  print_heading "Installing airflow dependencies for code completion"
  pip install -r $ROOT_DIR/airflow-requirements.txt --no-deps
}

update_python_deps() {
  print_heading "Compiling requirements"
  cd $ROOT_DIR
  CUSTOM_COMPILE_COMMAND="./scripts/install.sh -u" \
    pip-compile --upgrade --pip-args "--no-cache-dir"
}

install_workspace() {
  print_heading "Installing workspace $ROOT_DIR"
  pip3 install --no-deps --editable $ROOT_DIR
}

main() {
  UPDATE=0
  if [[ "$#" -ge 1 ]] && [[ "$1" = "-u" || "$1" = "update" ]]; then
    UPDATE=1
  fi

  print_heading "Installing workspace: ${ROOT_DIR}"
  print_heading "Installing pip-tools"
  python -m pip install pip-tools

  if [[ $UPDATE -eq 1 ]]; then
    print_horizontal_line
    update_python_deps
    print_horizontal_line
  fi

  install_python_deps
  print_horizontal_line
  install_workspace
  print_horizontal_line
}

main "$@"
