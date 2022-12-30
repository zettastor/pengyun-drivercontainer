#!/bin/bash 
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..

#java -noverify  -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.app.Launcher "spring-config/nbd_config.xml"

java -noverify -server -Xms500m -Xmx1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.drivercontainer.service.Launcher "spring-config/nbd_config.xml" 2>&1 > /dev/null
