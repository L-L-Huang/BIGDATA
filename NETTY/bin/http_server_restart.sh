#/bin/bash
##根据名称杀死进程
pkill HTTP_SERVER
##启动进程
echo "start up command:java -Xmx2g -Xms2g -Dlog_dir=/opt/logs -jar /opt/jar/HTTP_SERVER.jar 4 8 2048 80 > /dev/null 2>&1 &"
nohup java -Xmx2g -Xms2g -Dlog_dir=/opt/logs -jar /opt/jar/HTTP_SERVER.jar 4 8 2048 80 > /dev/null 2>&1 &
x