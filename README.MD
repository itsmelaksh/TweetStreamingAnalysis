This project is about reading streaming data and perform analysis
1. Netcat utility to run to enter data
    nc -lc <<port>>
2. spark-submit StreamProcess.py <<host>> <<port>>
   This will listen to the netcat input and perform counts