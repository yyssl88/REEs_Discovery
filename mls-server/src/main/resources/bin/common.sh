#!/bin/bash

APP_HOME=`cd $(dirname $0)/..; pwd -P`
#########################################################################################
#
# 获取环境相关信息
source ${APP_HOME}/bin/evn.sh

#########################################################################################
#
# 默认配置
APP_LOG_DIR="/var/log/seiois/${APP_NAME}"
# 配置文件目录路径
APP_CONF_DIR="${APP_HOME}/conf/"

# PID文件目录
APP_PID_DIR="/var/run/seiois/${APP_NAME}"
APP_PID_FILE_NAME="${APP_NAME}.pid"

# 系统环境: dev，开发环境；test，测试环境；pro，正式环境；
APP_EVN="dev"


# Java虚拟机启动参数
JAVA_OPTS="-Xms512m -Xmx512m -Xmn256m -Djava.awt.headless=true -XX:MaxPermSize=128m"

#执行程序启动所使用的系统用户，考虑到安全，推荐不使用root帐号
RUNNING_USER=root

#########################################################################################
#
# 获取配置信息
source ${APP_HOME}/conf/cfg.sh

mkdir -p $APP_LOG_DIR
mkdir -p $APP_PID_DIR

#拼凑完整的classpath参数，包括指定lib目录下所有的jar
CLASSPATH=$APP_HOME/classes
CLASSPATH="$CLASSPATH":"/etc/hadoop/conf"
for i in "$APP_HOME"/lib/*.jar; do
   CLASSPATH="$CLASSPATH":"$i"
done

###################################
#(函数)判断程序是否已启动
#
#说明：
#使用JDK自带的JPS命令及grep命令组合，准确查找pid
#jps 加 l 参数，表示显示java的完整包路径
#使用awk，分割出pid ($1部分)，及Java程序名称($2部分)
###################################
#初始化psid变量（全局）
psid=0

checkpid() {
   javaps=`$JAVA_HOME/bin/jps -l | grep $APP_MAINCLASS`

   if [ -n "$javaps" ]; then
      psid=`echo $javaps | awk '{print $1}'`
   else
      psid=0
   fi
}



###################################
#(函数)启动程序
#
#说明：
#1. 首先调用checkpid函数，刷新$psid全局变量
#2. 如果程序已经启动（$psid不等于0），则提示程序已启动
#3. 如果程序没有被启动，则执行启动命令行
#4. 启动命令执行后，再次调用checkpid函数
#5. 如果步骤4的结果能够确认程序的pid,则打印[OK]，否则打印[Failed]
#注意：echo -n 表示打印字符后，不换行
#注意: "nohup 某命令 >/dev/null 2>&1 &" 的用法
###################################
start() {
   echo "APP_LOG_DIR=$APP_LOG_DIR"
   echo "APP_NAME=$APP_NAME"
   echo "APP_MAINCLASS=$APP_MAINCLASS"

   checkpid

   if [ $psid -ne 0 ]; then
      echo "================================"
      echo "warn: ${APP_MAINCLASS} already started! (pid=${psid})"
      echo "================================"
   else
      echo -n "Starting ${APP_MAINCLASS} ..."
      JAVA_CMD="nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -classpath ${CLASSPATH} ${APP_MAINCLASS} "-cfg=${APP_CONF_DIR}" "-pid=${APP_PID_DIR}/${APP_PID_FILE_NAME}" "-evn=${APP_EVN}" > ${APP_LOG_DIR}/${APP_NAME}.log 2>&1 &"

      su - $RUNNING_USER -c "$JAVA_CMD"
      checkpid
      if [ $psid -ne 0 ]; then
         echo "(pid=$psid) [OK]"
         return 0
      else
         echo "[Failed]"
         return 1
      fi
   fi
}

###################################
#(函数)停止程序
#
#说明：
#1. 首先调用checkpid函数，刷新$psid全局变量
#2. 如果程序已经启动（$psid不等于0），则开始执行停止，否则，提示程序未运行
#3. 使用kill -9 pid命令进行强制杀死进程
#4. 执行kill命令行紧接其后，马上查看上一句命令的返回值: $?
#5. 如果步骤4的结果$?等于0,则打印[OK]，否则打印[Failed]
#6. 为了防止java程序被启动多次，这里增加反复检查进程，反复杀死的处理（递归调用stop）。
#注意：echo -n 表示打印字符后，不换行
#注意: 在shell编程中，"$?" 表示上一句命令或者一个函数的返回值
###################################
stop() {
   checkpid
 
   if [ $psid -ne 0 ]; then
      echo -n "Stopping $APP_MAINCLASS ...(pid=$psid) "
      su - $RUNNING_USER -c "kill $psid"
      if [ $? -eq 0 ]; then
         echo "[OK]"
      else
         echo "[Failed]"
      fi
 
      checkpid
      if [ $psid -ne 0 ]; then
         stop
      fi
   else
      echo "================================"
      echo "warn: $APP_MAINCLASS is not running"
      echo "================================"
   fi
}
 
###################################
#(函数)检查程序运行状态
#
#说明：
#1. 首先调用checkpid函数，刷新$psid全局变量
#2. 如果程序已经启动（$psid不等于0），则提示正在运行并表示出pid
#3. 否则，提示程序未运行
###################################
status() {
   checkpid
 
   if [ $psid -ne 0 ];  then
      echo "$APP_MAINCLASS is running! (pid=$psid)"
      return 0
   else
      echo "$APP_MAINCLASS is not running"
      return 1
   fi
}
 
###################################
#(函数)打印系统环境参数
###################################
info() {
   echo "System Information:"
   echo "****************************"
   echo `head -n 1 /etc/issue`
   echo `uname -a`
   echo
   echo "JAVA_HOME=$JAVA_HOME"
   echo `$JAVA_HOME/bin/java -version`
   echo
   echo "APP_HOME=$APP_HOME"
   echo "APP_MAINCLASS=$APP_MAINCLASS"
   echo "****************************"
}


###################################
#读取脚本的第一个参数($1)，进行判断
#参数取值范围：{start|stop|restart|status|info}
#如参数不在指定范围之内，则打印帮助信息
###################################
case "$1" in
   'start')
      start
      exit_code=$?
      exit "$exit_code"
      ;;
   'stop')
     stop
     ;;
   'restart')
     stop
     start
     exit_code=$?
     exit "$exit_code"
     ;;
   'status')
     status
     exit_code=$?
     exit "$exit_code"
     ;;
   'info')
     info
     ;;
  *)
     echo "Usage: $0 {start|stop|restart|status|info}"
     exit 1
esac

exit 0