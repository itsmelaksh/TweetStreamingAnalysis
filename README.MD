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
    
 
   