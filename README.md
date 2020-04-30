# Ads fraud detection using Kafka and Flink #
- - - - 
 ## Project goals ## 
 The goal of the project is to:

 * Create a Flink application which will read from Kafka `clicks` and `displays` queues
 * Detect some suspicious/fraudulent activities 
 * Output the suspicious events into a file.  
 
 ## Setting up project ## 
 Please get into the docker folder and run the docker-compose file.<br />
 To do so, simply execute these lines in the same directory as the docker-compose.yml file:  
 * `docker-compose rm -f` <br />
 * `docker-compose up`
 
 By running this docker-compose, it will :<br />
 * Create a Kafka cluster with two topics : “clicks” and “displays”
 * Launch a python script which will send events to those two topics and display them in the terminal   
 
 This generator simulates few suspicious/fraudulent patterns that you should detect using Flink.  There are 3 distincts patterns we want you to find.
