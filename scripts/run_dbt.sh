#!/bin/sh
set -eu

if [ -f ./.env ]; then
  set -a
  . ./.env
  set +a
fi

dbt build --project-dir dbt_project --profiles-dir dbt_project
