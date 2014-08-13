@echo off

set PROGRAM_DIR=%~dp0

set SOLR_HOME=-Dgeonames.solr.home="%PROGRAM_DIR%solr"
set PARAMS=-Dexec.args="%PROGRAM_DIR%%1"
set ENTRY_POINT=-Dexec.mainClass=com.googlecode.solrgeonames.harvester.Harvester
set MAVEN_OPTS=-XX:MaxPermSize=512m -Xmx1024m

call mvn %PARAMS% %SOLR_HOME% %ENTRY_POINT% exec:java
