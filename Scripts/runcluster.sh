#!/bin/bash

help ()
{
	echo "Usage: runcluster [options]

Options
 -m | --master		Runs on master node
 -s | --slave		Runs on slave node
 -S | --stop		Stops the node
 -f | --firstrun	Before starting, formats HDFS
 -h | --help		Displays help";
}

run ()
{
	# ZMIENNA JAVA HOME
	#export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-i386"

	# ZMIENNE (W CELU MODYFIKACJI NA INNEJ PLATFORMIE)
	export HADOOP_PREFIX="/macierz/home/143365sk/KAW/hadoop-2.6.0"
	export HADOOP_NAMENODE_OPTS="-XX:+UseParallelGC"
	export HADOOP_CLUSTER_NAME="KatWiki"
	export HADOOP_HOME=$HADOOP_PREFIX
	export HADOOP_COMMON_HOME=$HADOOP_PREFIX
	export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
	export HADOOP_HDFS_HOME=$HADOOP_PREFIX
	export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
	export HADOOP_YARN_HOME=$HADOOP_PREFIX

	# KOMENDY URUCHAMIAJACE HDFS
	# FORMAT FOLDERU NAMENODA -> NALEZY WYKONAC TYLKO RAZ, PODCZAS PIERWSZEGO URUCHOMIENIA
	if [[ $1 -eq 1 ]] ; then
		$HADOOP_PREFIX/bin/hdfs namenode -format
	fi
	# URUCHOMIENIE NAMENODA
	$HADOOP_PREFIX/sbin/hadoop-daemon.sh start namenode
	# UROCHOMIENIE DATANODE
	$HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode

	# KOMENDY URUCHAMIAJACE YARN
	# URUCHOMIENIE RESOURCE MANAGERA
	$HADOOP_PREFIX/sbin/yarn-daemon.sh start resourcemanager
	# URUCHOMIENIE NODE MANAGERA
	$HADOOP_PREFIX/sbin/yarn-daemon.sh start nodemanager
}

stop ()
{
	# ZMIENNA JAVA HOME
	#export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-i386"

	# ZMIENNE (W CELU MODYFIKACJI NA INNEJ PLATFORMIE)
	export HADOOP_PREFIX="/macierz/home/143365sk/KAW/hadoop-2.6.0"
	export HADOOP_NAMENODE_OPTS="-XX:+UseParallelGC"
	export HADOOP_CLUSTER_NAME="KatWiki"
	export HADOOP_HOME=$HADOOP_PREFIX
	export HADOOP_COMMON_HOME=$HADOOP_PREFIX
	export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
	export HADOOP_HDFS_HOME=$HADOOP_PREFIX
	export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
	export HADOOP_YARN_HOME=$HADOOP_PREFIX

	# KOMENDY ZATRZYMUJACE HDFS
	# ZATRZYMANIE NAMENODA
	$HADOOP_PREFIX/sbin/hadoop-daemon.sh stop namenode
	# ZATRZYMANIE DATANODE
	$HADOOP_PREFIX/sbin/hadoop-daemon.sh stop datanode

	# KOMENDY ZATRZYMUJACE YARN
	# ZATRZYMANIE RESOURCE MANAGERA
	$HADOOP_PREFIX/sbin/yarn-daemon.sh stop resourcemanager
	# ZATRZYMANIE NODE MANAGERA
	$HADOOP_PREFIX/sbin/yarn-daemon.sh stop nodemanager
}

master ()
{
	cp -R "./config/Master/core-site.xml" "./hadoop-2.6.0/etc/hadoop/core-site.xml"
	cp -R "./config/Master/yarn-site.xml" "./hadoop-2.6.0/etc/hadoop/yarn-site.xml"
}

slave ()
{
	cp -R "./config/Slave/core-site.xml" "./hadoop-2.6.0/etc/hadoop/core-site.xml"
	cp -R "./config/Slave/yarn-site.xml" "./hadoop-2.6.0/etc/hadoop/yarn-site.xml"
}

stopVar=0
slaveVar=0
firstrunVar=0

if [[ $# -eq 0 ]] ; then
	help
	exit 1
fi

while [ "$1" != "" ]; do
    case $1 in
        -m | --master )			slaveVar=0
                                ;;
        -s | --slave )			slaveVar=1
								;;
        -f | --firstrun )		firstrunVar=1
                                ;;
        -h | --help )
								help
                                exit 1
                                ;;
        -S | --stop )			stopVar=1
								;;
        * )                     help
                                exit 1
    esac
    shift
done

if [[ stopVar -eq 1 ]] ; then
	stop
	exit 0
else
	if [[ slaveVar -eq 1 ]] ; then
		slave
		run firstrunVar
		master
		exit 0
	else
		master
		run firstrunVar
		exit 0
	fi
fi

