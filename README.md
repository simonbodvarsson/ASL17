Advanced Systems Lab 2017 - Simon Bodvarsson

Overview of files and folders:

- The full report is contained in report.pdf
- The folder /report/Data/ contains the images used in the report as well as the data used to generate them. Each chapter of the report has its own subfolder containing its images and data.
  For each image, there is a folder with the same name containing the plotted data.
- The folder /report/Data/Data Extraction Scripts contains jupyter notebooks which were used to
generate images, tables and other results of the experiments.
- The folder /src/ contains the source Java code for the middleware.
- experiment-files.zip contains all measurements made in final experiments along with the bash
scripts used to run the experiments. Each experiment has its own folder with a subfolder for each
configuration used in the experiment. Inside these subfolders, the files client_NM.log and mw_NN.log contain the client and middleware output respectively. For the client_NM.log files, the number N represents the number of the VM the client was run on and M represents the instance of memtier on that machine (client_11.log and client12 were run on the same VM but not client_21.log and client31). Other files contain parts of the same data or the same data in a different format (e.g. JSON), their names should be self-explanatory.
- The Java classes in src/ are: 
    - MyMiddleware.java: The middleware class
    - NetHandler.java: Class which handles new client connections, parses and enqueuese requests
    - Worker.java: Removes requests from the Request queue, processes them if needed, sends them to the server and responds to client.
    - Request.java, SetRequests.java, GetRequest.java: Wrapper classes for each request type. Request.java is an abstract class which the other two extend.
    - Statisticshandler.java: Created on shut-down, the StatisticsHandler gathers statistics from all workers and logs them to files.