#!/bin/sh
#
# Copyright (c) 2012-2014 Andrea Selva
#

#
# this script has jmx and benchmark output
# to use this script, you need to modify the jmx config.
#

echo "                                                                         "
echo "  ___  ___                       _   _        ___  ________ _____ _____  "
echo "  |  \/  |                      | | | |       |  \/  |  _  |_   _|_   _| "
echo "  | .  . | ___   __ _ _   _  ___| |_| |_ ___  | .  . | | | | | |   | |   "
echo "  | |\/| |/ _ \ / _\ | | | |/ _ \ __| __/ _ \ | |\/| | | | | | |   | |   "
echo "  | |  | | (_) | (_| | |_| |  __/ |_| ||  __/ | |  | \ \/' / | |   | |   "
echo "  \_|  |_/\___/ \__, |\__,_|\___|\__|\__\___| \_|  |_/\_/\_\ \_/   \_/   "
echo "                   | |                                                   "
echo "                   |_|                                                   "
echo "                                                                         "

export JAVA_HOME=/opt/jdk1.7.0_79
export CLASSPATH=.:$JAVA_HOME/lib:$JAVA_HOME/jre/lib


cd "$(dirname "$0")"

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set MOQUETTE_HOME if not already set
[ -f "$MOQUETTE_HOME"/bin/moquette.sh ] || MOQUETTE_HOME=`cd "$PRGDIR/.." ; pwd`
export MOQUETTE_HOME

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

LOG_FILE=$MOQUETTE_HOME/config/logback.xml
MOQUETTE_PATH=$MOQUETTE_HOME/
#LOG_CONSOLE_LEVEL=info
#LOG_FILE_LEVEL=fine
JAVA_OPTS_SCRIPT="-XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true"

$JAVA -server -Xmx4096m -Xms4096m -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC -Xloggc:gc.log $JAVA_OPTS $JAVA_OPTS_SCRIPT -Dio.netty.allocator.type=pooled -Dmoquette.processor.benchmark=true -Dlogback.configurationFile="file:$LOG_FILE" -Djava.rmi.server.hostname=10.10.71.9 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10086 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dmoquette.path="$MOQUETTE_PATH" -cp "$MOQUETTE_HOME/lib/moquette-broker-0.7.1-final.jar:$MOQUETTE_HOME/lib/*" org.eclipse.moquette.server.Server


