# NoSQL Middleware - Advanced Systems Lab 2017 Project:

The project consists of *designing*, *developing* and *analyzing the performance of* a middleware platform for **key-value stores** (or NoSQL databases).

The middleware recieves requests from several clients (memtier instances) of type GET, SET or multi-GET. The requests are maintained in a queue until they are forwarded by one of the middleware's worker threads to one (or more) of the database servers. The middleware is responsible for replicating data between servers such that all database servers contain the same data in addition to handling load such that throughput and latency of requests is optimized.

A detailed description of middleware specifications and experiments performed for system analysis can be found in /report/report.pdf, /report/project20.pdf and /report/report-outline60.pdf.

## Overview of files and folders:

- The folder /src/ contains the source Java code for the middleware.
- The Java classes in src/ are:
    - `MyMiddleware.java`: The middleware class
    - `NetHandler.java`: Class which handles new client connections, parses and enqueues requests
    - `Worker.java`: Removes requests from the Request queue, processes them if needed, sends them to the server and responds to client.
    - `Request.java`, `SetRequests.java`, `GetRequest.java`: Wrapper classes for each request type. `Request.java` is an abstract class which the other two extend.
    - `Statisticshandler.java`: Created on shut-down, the `StatisticsHandler` gathers statistics from all workers and logs them to files.

- The full report is contained in report.pdf
- The folder /report/Data/ contains the images used in the report as well as the data used to generate them. Each chapter of the report has its own subfolder containing its images and data.
  For each image, there is a folder with the same name containing the plotted data.
- The folder /report/Data/Data Extraction Scripts contains jupyter notebooks which were used to
generate images, tables and other results of the experiments.
- experiment-files.zip contains all measurements made in final experiments along with the bash
scripts used to run the experiments. Each experiment has its own folder with a subfolder for each
configuration used in the experiment. Inside these subfolders, the files client_NM.log and mw_NN.log contain the client and middleware output respectively. For the client_NM.log files, the number N represents the number of the VM the client was run on and M represents the instance of memtier on that machine (client_11.log and client12 were run on the same VM but not client_21.log and client31). Other files contain parts of the same data or the same data in a different format (e.g. JSON), their names should be self-explanatory.
