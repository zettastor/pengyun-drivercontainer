#!/bin/bash 
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..
CLIENT_IP=$1

#java -noverify -server -Xms500m -Xmx1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.drivercontainer.utils.NBDDriverLauncher  $CLIENT_IP

java -noverify -server -Xms500m -Xmx1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.drivercontainer.driver.DriverLauncher --driver.type=NBD --driver.instance.id=123456789 --account.id=1 --driver.backend.volume.id=1 --driver.host.name=10.0.1.16 --driver.port=1234 --driver.coordinator.port=2234 
