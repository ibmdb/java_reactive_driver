#!/bin/bash

# find install dir
INSTALL_DIR=`pwd`
export INSTALL_DIR

export PATH=$INSTALL_DIR/bin:$PATH

# find r2dbc_db2 dependent jars
for jar in `ls $INSTALL_DIR/extlib/*.jar`
do
	if [ -n "$DEP_JARS" ]
	then
		DEP_JARS=$jar:$DEP_JARS
	else
		DEP_JARS=$jar
	fi
done
export DEP_JARS

# find r2dbc_db2 jars
for jar in `ls $INSTALL_DIR/lib/*.jar`
do
	if [ -n "$R2DBC_JARS" ]
	then
		R2DBC_JARS=$jar:$R2DBC_JARS
	else
		R2DBC_JARS=$jar
	fi
done
export R2DBC_JARS

# append the jars to the  CLASSPATH
CLASSPATH=.:$R2DBC_JARS:$DEP_JARS:$CLASSPATH
export CLASSPATH

