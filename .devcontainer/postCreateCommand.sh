#!/bin/bash
psql 'postgresql://postgres:postgres@db/postgres' -f './data/db.sql'
npm install
npm run build
npm run import:legislation & npm run import:static