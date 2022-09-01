#!/bin/bash


### run this script from project root ###
docker-compose -f src/docker/docker-compose.yml down
rm -f target/waves-enterprise-all*.jar
sbt assembly
docker-compose -f src/docker/docker-compose.yml build
