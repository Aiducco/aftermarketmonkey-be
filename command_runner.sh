#!/bin/bash

PROJECT_DIR="/root/aftermarketmonkey_be"

VENV_DIR="/root/aftermarketmonkey_be/venv"

if [ -z "$1" ]; then
  echo "Usage: $0 <management_command>"
  exit 1
fi

COMMAND=$1

source $VENV_DIR/bin/activate

export DJANGO_SETTINGS_MODULE=settings
export DISPLAY=:99

cd $PROJECT_DIR

python manage.py $COMMAND

deactivate
