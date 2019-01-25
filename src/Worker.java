import java.util.*;
import java.util.concurrent.*;
import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;

// The worker removes Requests from the requestQueue whenever they are available.
// The worker then gathers various statistics on the requests seen before forwarding
// the request to the servers. 

// Every 4 seconds the worker adds all gathered statistics to lists which contain the
// statistics of 4 second windows during the workers run time. These statistics are
// then retrieved by the StatisticsHandler.

// If readSharded == True, the worker will break up GET requests and send the parts to 
// different servers. When the servers have replied, it will construct a single 
// reply to send to the initial client.

public class Worker implements Runnable {
	// Instance variables
	private BlockingQueue<Request> requestQueue = null;
	private List<String> mcAddresses = null;
	private boolean readSharded = false;
	private String threadName;
	private List<Socket> serverSocketList = null;
	private int serverNum = 0;
	private int serverToUse = 0;


	// Statistics measurements for the current time window:
	private long measurementStartTime;
	private long lastTime;
	private int currentIteration = 1;

	public int getCount = 0;
	public int multigetCount = 0;
	public int setCount = 0;

	private int currentMisses = 0;
	public int getMisses = 0;

	public int queueLengthSum = 0;

	public long lastStatsTime;
	public int lastStatsRequests;

	public long QueueWaitingTimeSum = 0;
	public long QueueWaitingTimeSqSum = 0;

	public long getServiceTimeSum = 0;
	public long getServiceTimeSqSum = 0;

	public long multigetServiceTimeSum = 0;
	public long multigetServiceTimeSqSum = 0;

	public long setServiceTimeSum = 0;
	public long setServiceTimeSqSum = 0;


	// Lists of measurements for time windows:
	public List<Long> timeStamps = new ArrayList<>(2048);
	public List<Float> throughput = new ArrayList<>(2048);

	public List<Integer> reqCounts = new ArrayList<>(2048);
	public List<Integer> getCounts = new ArrayList<>(2048);
	public List<Integer> multigetCounts = new ArrayList<>(2048);
	public List<Integer> setCounts = new ArrayList<>(2048);

	public List<Long> queueWaitingTimeSums = new ArrayList<>(2048);
	public List<Long> queueWaitingTimeSqSums = new ArrayList<>(2048);
	
	public List<Long> getServiceTimeSums = new ArrayList<>(2048);
	public List<Long> getServiceTimeSqSums = new ArrayList<>(2048);
	
	public List<Long> multigetServiceTimeSums = new ArrayList<>(2048);
	public List<Long> multigetServiceTimeSqSums = new ArrayList<>(2048);
	
	public List<Long> setServiceTimeSums = new ArrayList<>(2048);
	public List<Long> setServiceTimeSqSums = new ArrayList<>(2048);

	public List<Integer> avgQueueLength = new ArrayList<>(2048);

	// responseTimeHist[i] = #requests which had response time t,
	// s.t. (i-1)*100 microseconds < t < i*100 microseconds
	public long maxResponseTime = -1;
	public long minResponseTime = -1;
	public int[] responseTimeHist = new int[1024];

	public List<String> errors = new ArrayList<>();

	// Sum of number of keys in all requests
	public int keynumsum = 0;
	private int current_keynumsum = 0;
	

	// Constructor
	Worker(BlockingQueue<Request> requestQueue, List<String> mcAddresses, boolean readSharded) {
		this.requestQueue = requestQueue;
		this.mcAddresses = mcAddresses;
		this.readSharded = readSharded;
		this.serverSocketList = new ArrayList<Socket>(mcAddresses.size());
		this.serverNum = mcAddresses.size();
		// Round robin server selection
		this.serverToUse = 0;
	}


	// Methods
	public void run() {
		// Set up
		this.threadName = Thread.currentThread().getName();
		
		connectToServers();

		this.measurementStartTime = System.nanoTime();
		this.lastTime = measurementStartTime;
		this.lastStatsRequests = 0;

		// Remove and handle requests from RequestQueue
		while(true) {
			try{
				long currentTime = System.nanoTime();
				long timePassed = (currentTime - this.lastTime)/1000000L;
				

				// Check if 4 seconds have passed for this iteration
				if ((currentTime - this.measurementStartTime)/1000000L > this.currentIteration*4000) {
					currentIteration++;
					// Gather statistics for the timewindow
					gatherAggregate(timePassed, currentTime);
				}

				// Handle request
				Request request = requestQueue.take();
				if (request == null) {
					break;
				} else {
					gatherStats(request);
					if (request instanceof SetRequest) {
						sendSetRequest(request);
					} else {
						sendGetRequest(request);
					}

					// Increment number of requests seen in this time window
					lastStatsRequests++;
				
				}
			} catch (Exception e) {
				System.out.println("Error in worker thread");
				System.out.println(Thread.currentThread().getName() 
             	+ " " + e.getMessage());
             	e.printStackTrace();
             	
             	this.errors.add("Thread " + Thread.currentThread().getName() 
             	+ " " + e.getMessage());
			}
		}
		return;
	}

	// Add measurements for current timewindow to lists of measurements
	private void gatherAggregate(long timePassed, long currentTime){
		this.lastTime = currentTime;
		// System.out.println("===========================================================");
		// System.out.println("Gathering Aggregates: ");
		// System.out.println("Time since last aggregate (ms): " + timePassed);
		// System.out.println("Requests since last aggregate: " + this.lastStatsRequests);
		float t = ((float) lastStatsRequests)/timePassed*1000;
		// System.out.println("Throughput: " + t +  " req/s");

		// Add to lists
		this.throughput.add(t);
		this.reqCounts.add(this.lastStatsRequests);
		this.multigetCounts.add(this.multigetCount);
		this.getCounts.add(this.getCount);
		this.setCounts.add(this.setCount);
		this.getMisses += this.currentMisses;

		this.queueWaitingTimeSums.add(this.QueueWaitingTimeSum);
		this.queueWaitingTimeSqSums.add(this.QueueWaitingTimeSqSum);
		this.getServiceTimeSums.add(this.getServiceTimeSum);
		this.getServiceTimeSqSums.add(this.getServiceTimeSqSum);
		this.multigetServiceTimeSums.add(this.multigetServiceTimeSum);
		this.multigetServiceTimeSqSums.add(this.multigetServiceTimeSqSum);
		this.setServiceTimeSums.add(this.setServiceTimeSum);
		this.setServiceTimeSqSums.add(this.setServiceTimeSqSum);
		this.avgQueueLength.add(queueLengthSum/this.lastStatsRequests);
		this.keynumsum += this.current_keynumsum;
		
		// Reset all counts
		this.lastStatsRequests = 0;
		this.setCount = 0;
		this.getCount = 0;
		this.multigetCount = 0;
		this.currentMisses = 0;

		this.queueLengthSum = 0;
		this.QueueWaitingTimeSum = 0;
		this.QueueWaitingTimeSqSum = 0;
		this.getServiceTimeSum = 0;
		this.getServiceTimeSqSum = 0;
		this.multigetServiceTimeSum = 0;
		this.multigetServiceTimeSqSum = 0;
		this.setServiceTimeSum = 0;
		this.setServiceTimeSqSum = 0;
		this.current_keynumsum = 0;

		// Record timestamp
		long timestamp = currentTime;
		this.timeStamps.add(timestamp);
	}

	// Gather statistics for a single request
	private void gatherStats(Request request) {

		if( request instanceof SetRequest) this.setCount++;
		else if(request instanceof GetRequest) {
			if (((GetRequest) request).keyNum > 1) this.multigetCount++;
			else this.getCount++;
		}
		long queueTime = (System.nanoTime() - request.enqueueTime)/1000L;
		this.QueueWaitingTimeSum += queueTime;
		this.QueueWaitingTimeSqSum += queueTime*queueTime;
		this.queueLengthSum += this.requestQueue.size();
	}

	// Forward Set request to servers
	private void sendSetRequest(Request request) throws IOException {
		//System.out.println("SET REQUEST: " + request.requestString);
		// Send request to all servers
		for (int i=0; i<this.serverNum; i++) {
			Socket socket = this.serverSocketList.get(i);
			OutputStream out = socket.getOutputStream();
			out.write(request.requestString.getBytes());
			out.flush();
		}

		// Measure service time
		long serviceTimeBegin = System.nanoTime();
		

		String clientResponse = "";
		boolean error = false;

		// Read response from all servers
		for (int i=0; i<this.serverNum; i++) {
			Socket socket = this.serverSocketList.get(i);
			InputStream inStream = socket.getInputStream();
			BufferedReader in = 
						new BufferedReader(
							new InputStreamReader(inStream));

			String response = in.readLine()+"\r\n";
			String line = null;
			while( (in.ready()) && ((line = in.readLine())  != null)) {
				response = response + line + "\r\n";
			}
			//System.out.println("SET Response: " + response);
			// Check for errors. If an error is returned from any server, we consider the
			// request to have failed.
			if ((!response.equals("STORED\r\n")) && (response.equals("NOT_STORED\r\n"))) {
				System.out.println("ERROR OCCURRED: " + response);
				error = true;
				clientResponse = response;
			}
			if (error == false) {
				clientResponse = response;
			}
		}
		
		long serviceTime = (System.nanoTime() - serviceTimeBegin)/1000L;
		this.setServiceTimeSum += serviceTime;
		this.setServiceTimeSqSum += serviceTime*serviceTime;
		
		// Send response to client
		answerClient(request, clientResponse);
		return;
	}

	// Forward Get request to servers
	private void sendGetRequest(Request g_request) throws IOException {
		//System.out.println("GET REQUEST: " + g_request.requestString);
		GetRequest request = (GetRequest) g_request;
		// System.out.println("NumKeys in getRequest: " + request.keyNum);

		// Handle Multigets if using sharded reads.
		if ((this.readSharded == true) && (request.keyNum > 1)) {
			sendShardedGetRequest(request);

		} else {
			// Rotate which servers to send request to.
			this.serverToUse = (this.serverToUse+1)%this.serverNum;

			Socket socket = this.serverSocketList.get(serverToUse);
			// Write to this stream
			OutputStream out = socket.getOutputStream();
			// Read from this stream
			InputStream inStream = socket.getInputStream();

			// Send request to server
			out.write(request.requestString.getBytes());
			out.flush();

			// Measure service time
			long serviceTimeBegin = System.nanoTime();

			// Read response from server
			BufferedReader in = 
				new BufferedReader(
					new InputStreamReader(inStream));


			String response = "";
			String line = null;
			while( true){
				line = in.readLine();
				response = response + line + "\r\n";
				//System.out.print("Line" + response);
				if (line.startsWith("END")) {
					response = response;
					break;
				}
			}
								// // Reconstruct answer from read lines
								// String response = in.readLine()+"\r\n";
								// String line = null;
								// while( (in.ready()) && ((line = in.readLine())  != null)) {
								// 	// 
								// 	response = response + line + "\r\n";
								// }

			if (response.equals("END\r\n")) {
				// System.out.println("GET MISS!");
				this.currentMisses++;
			}
			//System.out.println("GET RESPONSE: " + response);

			long serviceTime = (System.nanoTime() - serviceTimeBegin)/1000L;

			// Update multiget service time
			if (request.keyNum > 1) {
				// System.out.println("Multi get service time: " + serviceTime);
				this.multigetServiceTimeSum += serviceTime;
				this.multigetServiceTimeSqSum += serviceTime*serviceTime;
				this.current_keynumsum += request.keyNum;
				
			}

			// Update get service time
			else {
				// System.out.println("get service time: " + serviceTime);
				this.getServiceTimeSum += serviceTime;
				this.getServiceTimeSqSum += serviceTime*serviceTime;
			}

			// Send response to client
			answerClient(request, response);	
		}			
		return;
	}

	// Handle and forward sharded get request.
	private void sendShardedGetRequest(Request g_request) throws IOException {
		GetRequest request = (GetRequest) g_request;
		int nShards = Math.min(request.keyNum, this.serverNum);
		String[] req = new String[nShards];
		int shardLength = request.keyNum/nShards;
		int keyIndex = 0;
		
		// Create requests
		for (int i=0; i<req.length; i++) {
			req[i] = "get";
			for (int j=0; j<shardLength; j++) {
				req[i] += " " + request.keys[keyIndex] ;
				keyIndex++;
			}

			// Add remaining keys to last request
			if(i == req.length-1) {
				while( keyIndex < request.keyNum){
					req[i] += " " + request.keys[keyIndex] ;
					keyIndex++; 
				}
			}
			// Add eol for all but last request.
			if (i<req.length-1) {				
				req[i] += "\r\n";
			}
			// System.out.println("req[i]: " + req[i]);
		}

		int[] serversToUse = new int[nShards];
		for (int i=0; i<nShards; i++) {
			this.serverToUse = (this.serverToUse+1)%this.serverNum;
			serversToUse[i] = this.serverToUse;
		}
		// send all requests to servers
		for( int i=0; i<req.length; i++){
			// System.out.println("Sending requestShard " + i + " to server " + serversToUse[i]);
			Socket socket = this.serverSocketList.get(serversToUse[i]);
			OutputStream out = socket.getOutputStream();
			out.write(req[i].getBytes());
			out.flush();
		}
		long serviceTimeBegin = System.nanoTime();

		// gather all responses and create one final response for client
		String clientResponse = "";
		for (int i=0; i<req.length; i++) {
			Socket socket = this.serverSocketList.get(serversToUse[i]);
			InputStream inStream = socket.getInputStream();
			BufferedReader in = 
				new BufferedReader(
					new InputStreamReader(inStream));

			String response = in.readLine()+"\r\n";
			String line = null;

			while( (in.ready()) && ((line = in.readLine())  != null)) {
				response = response + line + "\r\n";
			}

			// remove EOL string "END\r\n"
			response = response.substring(0,response.length()-5);
			clientResponse += response;
			// System.out.println("Server "+  serversToUse[i] + " response: " + response);
		}

		long serviceTime = (System.nanoTime() - serviceTimeBegin)/1000L;
		// System.out.println("sharded multiget service time: " + serviceTime);
		this.multigetServiceTimeSum += serviceTime;
		this.multigetServiceTimeSqSum += serviceTime*serviceTime;
		this.current_keynumsum += request.keyNum;

		clientResponse += "END\r\n";
		
		int returnedKeys = (clientResponse.split("\r\n").length - 1)/2;
		if( request.keyNum > returnedKeys) {
			// System.out.println("Miss: " + request.keyNum + " VS " + returnedKeys);
			this.currentMisses++;
		}

		answerClient(request, clientResponse);
		return;
	}


	// Send response to client
	private void answerClient(Request request, String response) throws IOException {
		final byte[] respB = response.getBytes();
		ByteBuffer buf = ByteBuffer.allocate(respB.length);
		buf.clear();
		buf.put(respB);
		buf.flip();
		// System.out.println("Answering client: " + response);
		while(buf.hasRemaining()) {
			request.sc.write(buf);
		}
		measureResponseTime(request);
		return;
	}

	private void measureResponseTime(Request request) {
		long responseTime = (System.nanoTime() - request.enqueueTime)/1000L;
		int histogramIndex = ((int) responseTime) / 100;
		// System.out.println("Response time: " + responseTime);
		// System.out.println("histogramIndex: " + histogramIndex);
		if( this.maxResponseTime == -1){
			this.maxResponseTime = responseTime;
			this.minResponseTime = responseTime;
		}
		this.maxResponseTime = Math.max(this.maxResponseTime, responseTime);
		this.minResponseTime = Math.min(this.minResponseTime, responseTime);

		while (histogramIndex >= responseTimeHist.length) {
			int[] newHistogram = new int[this.responseTimeHist.length*2];
			System.arraycopy(this.responseTimeHist, 0, newHistogram, 0, responseTimeHist.length);
			this.responseTimeHist = newHistogram;
		}

		this.responseTimeHist[histogramIndex] += 1;
		return;
	}

	// Connect to all servers
	private void connectToServers(){
		// System.out.println("Connecting to "+ mcAddresses.size() +" servers: ");
		try{
			for (int i=0; i<mcAddresses.size(); i++) {
				// System.out.print(i + ": ");
				int sepIndex = mcAddresses.get(i).indexOf(":");

				String host = mcAddresses.get(i).substring(0,sepIndex);
				int port = Integer.parseInt(mcAddresses.get(i).substring(sepIndex+1));
				
				// System.out.println( host + ":" + port);
				
				Socket socket = new Socket(host, port);
				this.serverSocketList.add(socket);
				
			}
		} catch (IOException e) {
			this.errors.add("Thread " + Thread.currentThread().getName() 
             	+ " " + e.getMessage());
			System.out.println(e.getMessage());
		}
	}
}