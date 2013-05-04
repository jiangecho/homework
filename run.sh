rm -rf /output
hadoop fs -rm /input/fileList.list
hadoop fs -rmr /output
#ls -1 $1 |awk '{print i$1$0}' i=`pwd`'/' > fileList.list
hadoop fs -put fileList.list /input/fileList.list
hadoop fs -rmr /conf
hadoop fs -mkdir /conf
hadoop fs -put features.xml /conf/features.xml
hadoop jar extract.jar /input/fileList.list /output
rm -rf ./output
hadoop fs -get /output .

