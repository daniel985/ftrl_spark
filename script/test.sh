#!/bin/bash
#########################################################################
# File Name: test.sh
# Author: daniel.wang
# Mail: wangzhanoop@163.com
# Created Time: Tue 11 Sep 2018 09:51:52 AM CST
# Brief: 
#########################################################################
day="2018-09-11"
spark_submit=/usr/bin/spark2-submit
trainConf=ftrl.conf
initModelPath=/user/init_model
modelPath=/user/model
trainPath=/user/train
evalPath=/user/test
auc=/user/result

hdfs dfs -rm -r ${trainPath}
${spark_submit} \
    --master yarn \
    --driver-memory 4g \
    --executor-memory 16G \
    features.py ${day}

hdfs dfs -rm -r ${modelPath}
hdfs dfs -rm -r ${auc}
${spark_submit} --master yarn \
	--driver-memory 4g \
	--executor-memory 16G \
	--files ftrl.conf,fea.filter \
	--class spark.ftrl.FtrlTrain \
	./target/spark_ftrl-0.4-jar-with-dependencies.jar \
	${trainConf} \
	${feaFilter} \
	${initModelPath} \
	${modelPath} \
	${trainPath} \
	${evalPath} \
	${auc} \
    ${partLimit}

hdfs dfs -cat ${auc}/part-00000
