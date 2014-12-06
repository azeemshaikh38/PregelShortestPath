hadoop namenode -format
../hadoop-1.0.4/bin/start-all.sh
../spark-1.1.0-bin-hadoop1/sbin/start-all.sh

hadoop fs -mkdir /user/ubuntu/GoogleGraphShortestPath/

for i in `seq 13 20`;
do

let "tmp=2**$i"
cd Inputs/
perl binaryTree.pl $tmp
hadoop fs -rmr /user/ubuntu/GoogleGraphShortestPath/binaryTree*
hadoop fs -put ./binaryTree.txt /user/ubuntu/GoogleGraphShortestPath/

cd ../

hadoop fs -rmr /user/ubuntu/GoogleGraphShortestPath/edgesObject_*
hadoop fs -rmr /user/ubuntu/GoogleGraphShortestPath/verticesObject_*

time ../spark-1.1.0-bin-hadoop1/bin/spark-submit --class "ShortestPath" --master spark://10.0.3.25:7077 --conf "-Dlog4j.configuration=file:///home/ubuntu/ShortestPath/log4j.properties spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps spark.executor.memory=8G spark.driver.memory=8G" target/scala-2.10/simple-project_2.10-1.0.jar

echo "Finished $tmp";
done
