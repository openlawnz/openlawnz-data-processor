#!/bin/bash
psql 'postgresql://postgres:postgres@db/postgres' -f './data/db.sql'
mkdir .case-cache
mkdir .search-cache
echo LOCAL_CASE_PATH="$(pwd)/.case-cache" > .env
npm install
npm run build
npm run import:legislation & npm run import:static