#!/bin/sh

status(){
CNT=`ps -ef |grep "python3 $1.py"|grep -v grep|wc -l`
if [ $CNT -eq 0 ];then
printf "%-30s [stoped]\n" $1
else
printf "%-30s [running]\n" $1
fi
}

start(){
CNT=`ps -ef |grep "python3 $1.py"|grep -v grep|wc -l`
if [ $CNT -eq 0 ];then
nohup python3 $1.py &>running.log &
printf "%-30s [starting]\n" $1
else
printf "%-30s [running]\n" $1
fi
}

stop(){
CNT=`ps -ef |grep "python3 $1.py"|grep -v grep|wc -l`
if [ $CNT -eq 1 ];then
    PID=`ps -ef |grep "python3 $1.py"|grep -v grep|awk '{print $2}'`
    kill $PID
    for i in `seq 60`
    do
        sleep 1
        CNT=`ps -ef |grep "python3 $1.py"|grep -v grep|wc -l`
        if [ $CNT -eq 0 ];then
        printf "%-30s [stoped]\n" $1
        exit 1
        fi
    done
	printf "%-30s [stopging timeout,you can try:kill -9 $PID]\n" $1
elif [ $CNT -eq 0 ];then
    printf "%-30s [stoped]\n" $1
else  
    printf "%-30s [unkown]\n" $1
fi
}


#主程序

case "$1" in
  'status')
    status archiver
    ;;
  'start')
    start archiver
    ;;
  'stop')
    stop archiver
    ;;
  *)
    echo "Usage: ./admin.sh {start|stop|status}" 
    exit 1
    ;;
esac

