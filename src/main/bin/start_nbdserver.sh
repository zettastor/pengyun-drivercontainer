#!/bin/bash 
# Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..
CLIENT_IP=$1

#java -noverify -server -Xms500m -Xmx1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.drivercontainer.utils.NBDDriverLauncher  $CLIENT_IP

java -noverify -server -Xms500m -Xmx1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.drivercontainer.driver.DriverLauncher --driver.type=NBD --driver.instance.id=123456789 --account.id=1 --driver.backend.volume.id=1 --driver.host.name=10.0.1.16 --driver.port=1234 --driver.coordinator.port=2234 
