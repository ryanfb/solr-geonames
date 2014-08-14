#!/bin/bash
PROGRAM_DIR=`cd \`dirname $0\`; pwd`
SOLR_HOME="-Dgeonames.solr.home=$PROGRAM_DIR/solr"
export MAVEN_OPTS="-XX:MaxPermSize=512m -Xmx1024m"
JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8" nohup mvn -P dev $SOLR_HOME jetty:run &> stdout.log
