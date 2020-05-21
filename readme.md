mvn clean install verify

/home/dj/Downloads/flink-1.10.0/bin/start-cluster.sh 

sudo mongod --port 27017 --dbpath /home/dj/database/quants

/home/dj/Downloads/flink-1.10.0/bin/flink run -c exploration.App target/flink-1.0-SNAPSHOT-jar-with-dependencies.jar 6

/home/dj/Downloads/flink-1.10.0/bin/stop-cluster.sh 


https://github.com/yougov/mongo-connector