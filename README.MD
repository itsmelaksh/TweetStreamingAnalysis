This project is about reading streaming data and perform analysis
1. Netcat utility to run to enter data
    nc -lc <<port>>
2. spark-submit StreamProcess.py <<host>> <<port>>
   This will listen to the netcat input and perform counts
   
### Steps to Run the program 
    ## Run the command: python main.py
    ## Then start the application at host and port http://localhost:9009/
    ## Then run the following command: spark-submit dataprocessing.py
    ## and, then start the api application to receive data from spark layer
    

# Download latest or other version of kafka 
# Extract it into folder and go to that folder to start zookeepeer and kafka server
# start zookeeper
sh bin/zookeeper-server-start.sh config/zookeeper.properties

# start kafka
sh bin/kafka-server-start.sh config/server.properties

# Start receiving messages from Tweeter and then process it to kafka
python main.py
# start receiving messages using spark
spark-submit --jars spark.jar receivemessage.py

# Start api to show the graph
python api.py