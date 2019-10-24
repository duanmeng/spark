#! /usr/bin/env bash

git submodule init
CUR_DIR="$(cd "`dirname "$0"`"; pwd)"
git submodule update --remote

cd "${CUR_DIR}"/tdw-spark-toolkit
mvn -DskipTests clean package install -U

cd "${CUR_DIR}"
# mvn -Pyarn -Phadoop-2.2-tdw -Dhadoop.version=2.2.0-tdw-0.2.270 -DskipTests clean package
./dev/make-distribution.sh  --name tdw --pip --tgz  -Pmesos -Pyarn -Pkubernetes -Phadoop-2.2-tdw -Dhadoop.version=2.2.0-tdw-0.2.270