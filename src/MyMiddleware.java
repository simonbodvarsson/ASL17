import java.util.*;
import java.util.concurrent.*;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.io.FileNotFoundException;
// -l <MyIP> -p <MyListenPort> -t <NumberOfThreadsInPool> -s <readSharded> -m <MemcachedIP:Port> <MemcachedIP2:Port2>
// memcached -p 1234 -vv
// memcached -p 1235 -vv
// memcached -p 1236 -vv
// java RunMW -l 192.12.13.15 -p 6379 -t 1 -s true -m 127.0.0.1:1234 127.0.0.1:1235 127.0.0.1:1236
// memtier_benchmark -t 1 -c 20 -n 10000 --multi-key-get=100 --ratio=1:20 -d 128 -P memcache_text
// memtier_benchmark -t 1 -c 20 -n 30000 --multi-key-get=6 --ratio=1:6 -d 1024 -P memcache_text

// For mac: 
// docker run -t --rm memtier_benchmark -s docker.for.mac.localhost -p 12345 -P memcache_text -n 100000 -c 2 -t 1 --expiry-range=9999-10000 --key-maximum=10000 --multi-key-get=10 --hide-histogram

public class MyMiddleware {
		final String myIp;
		final int myPort;
		final List<String> mcAddresses;
		final int numThreadsPTP;
		final boolean readSharded;
		private List<Worker> workers;
		private List<Thread> workerThreads;

		// MyMiddleware construtor
		public MyMiddleware(String myIp, int myPort, List<String> mcAddresses, 
			int numThreadsPTP, boolean readSharded) {
			this.myIp = myIp;
			this.myPort = myPort;
			this.mcAddresses = mcAddresses;
			this.numThreadsPTP = numThreadsPTP;
			this.readSharded = readSharded;
			this.workers = new ArrayList<>(numThreadsPTP);
			this.workerThreads = new ArrayList<>(numThreadsPTP);
		}

		public void run() {
			System.out.println("Middleware started:");
			System.out.println("Worker threads: "+ numThreadsPTP);
			System.out.println("Servers: " + mcAddresses.size());
			System.out.println("Sharded reads: " + readSharded);
			
			// NetThread accepts new connections and listens for complete requests.
			LinkedBlockingQueue<Request> requestQueue = new LinkedBlockingQueue();
			NetHandler netHandler = new NetHandler(myPort, requestQueue);
			Thread netThread = new Thread(netHandler, "Net Thread");

			// Create workers
			for (int i=0; i<numThreadsPTP; i++) {
				Worker w = new Worker(requestQueue, mcAddresses, readSharded);
				String wtName = new String("Worker thread " + i);
				Thread wt = new Thread(w, wtName);
				workers.add(w);
				workerThreads.add(wt);
			}

			// start all threads
			netThread.start();

			for (Thread w:workerThreads ) {
				w.start();
			}

			// Log statistics to file
			Runtime.getRuntime().addShutdownHook(
				new Thread(
					new StatisticsHandler(workers, netHandler, mcAddresses.size(), readSharded)));
		}
}
