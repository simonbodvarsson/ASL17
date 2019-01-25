import java.util.*;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.nio.file.*;
import java.io.File;

// Gathers and calculates statistics from all worker threads when middleware is shut down.
public class StatisticsHandler implements Runnable {
	private List<Worker> workers;
	private int n_workers;
	private int n_servers;
	private boolean shardedreads;

	private NetHandler netHandler;
	private List<Long> timeStamps;
	private List<Integer> avgQueueLength;
	private List<Integer> avgThroughput = new ArrayList<>(1024);
	private List<Long> varThroughput = new ArrayList<>(1024);
	private List<Long> avgQueueWaitTime = new ArrayList<>(1024);
	private List<Long> varQueueWaitTime = new ArrayList<>(1024);

	private List<Long> avgGetServiceTime = new ArrayList<>(1024);
	private List<Long> varGetServiceTime = new ArrayList<>(1024);

	private List<Long> avgSetServiceTime = new ArrayList<>(1024);
	private List<Long> varSetServiceTime = new ArrayList<>(1024);

	private List<Long> avgMultigetServiceTime = new ArrayList<>(1024);
	private List<Long> varMultigetServiceTime = new ArrayList<>(1024);

	private List<Integer> getOps = new ArrayList<>(1024);
	private List<Integer> multigetOps = new ArrayList<>(1024);
	private List<Integer> setOps = new ArrayList<>(1024);
	private float missRatio;
	private float avgKeyNum;

	private int[] responseTimeHist;
	private long maxResponseTime;
	private long minResponseTime;

	private String[][] measurementTable;

	public StatisticsHandler(List<Worker> workers, NetHandler netHandler,
			int n_servers, boolean shardedReads) {
		this.workers = workers;
		this.n_workers = workers.size();
		this.netHandler = netHandler;
		this.n_servers = n_servers;
		this.shardedreads = shardedReads;
	}

	public void run() {
		System.out.println("Statistics Handler starting");
		printErrors();
		getStatistics();
		printHistogram();
		printStatistics();
		logStatistics();
		return;
	}

	private void getStatistics(){
		int totalGetReqs = 0;
		int totalMultigetReqs = 0;
		int misses = 0;
		int keynumsum = 0;

		this.timeStamps = workers.get(0).timeStamps;
		this.avgQueueLength = workers.get(0).avgQueueLength;
		this.responseTimeHist = new int[workers.get(0).responseTimeHist.length];
		this.maxResponseTime = workers.get(0).maxResponseTime;
		this.minResponseTime = workers.get(0).minResponseTime;

		for (int i=0; i<workers.size(); i++) {
			Worker w = workers.get(i);

			misses += w.getMisses;
			keynumsum += w.keynumsum;

			for (int j=0; j<w.timeStamps.size(); j++) {
				// Initialize if this is the first worker
				if (i == 0) {
					this.getOps.add(j, 0);
					this.multigetOps.add(j,0);
					this.setOps.add(j,0);
					this.avgThroughput.add(j,0);
					this.avgQueueWaitTime.add(j, (long) 0.0);
					// this.avgServiceTime.add(j, (long) 0.0); 
					this.avgGetServiceTime.add(j, (long) 0.0);
					this.avgSetServiceTime.add(j, (long) 0.0);
					this.avgMultigetServiceTime.add(j, (long) 0.0);
				}
				int gets = this.getOps.get(j);
				int multigets = this.multigetOps.get(j);
				int sets = this.setOps.get(j);
				int throughput = this.avgThroughput.get(j);
				long waitTime = this.avgQueueWaitTime.get(j);
				// long serviceTime = this.avgServiceTime.get(j);
				long getServiceTime = this.avgGetServiceTime.get(j);
				long setServiceTime = this.avgSetServiceTime.get(j);
				long multigetServiceTime = this.avgMultigetServiceTime.get(j);

				gets += w.getCounts.get(j);
				multigets += w.multigetCounts.get(j);
				sets += w.setCounts.get(j);

				int currentReqs = gets+sets+multigets;
				totalGetReqs += gets+multigets;
				totalMultigetReqs += multigets;

				throughput += (w.throughput.get(j)/this.n_workers);
				waitTime += ((w.queueWaitingTimeSums.get(j)/currentReqs)/this.n_workers);
				getServiceTime += ((w.getServiceTimeSums.get(j)/Math.max(gets,1))/this.n_workers);
				setServiceTime += ((w.setServiceTimeSums.get(j)/Math.max(sets,1))/this.n_workers);
				multigetServiceTime += ((w.multigetServiceTimeSums.get(j)/Math.max(multigets,1))/this.n_workers);

				this.getOps.set(j,gets);
				this.multigetOps.set(j,multigets);
				this.setOps.set(j,sets);
				this.avgThroughput.set(j,throughput);
				this.avgQueueWaitTime.set(j, waitTime);
				this.avgGetServiceTime.set(j, getServiceTime);
				this.avgSetServiceTime.set(j, setServiceTime);
				this.avgMultigetServiceTime.set(j, multigetServiceTime);
			}

			// Get response times
			if (this.responseTimeHist.length < w.responseTimeHist.length) {
				int[] newHistogram = new int[w.responseTimeHist.length];
				System.arraycopy(this.responseTimeHist, 0, newHistogram, 0, 
					this.responseTimeHist.length);
				this.responseTimeHist = newHistogram;
			}

			this.maxResponseTime = Math.max(this.maxResponseTime, w.maxResponseTime);
			this.minResponseTime = Math.min(this.minResponseTime, w.minResponseTime);

			for (int j=0; j<w.responseTimeHist.length; j++) {
				// Add response time of worker to histogram
				this.responseTimeHist[j] += w.responseTimeHist[j];
			}
		}
		this.missRatio = ((float) misses)/((float) totalGetReqs);
		if(totalMultigetReqs>0){
			this.avgKeyNum = ((float) keynumsum) / ((float) totalMultigetReqs);	
		} else {
			this.avgKeyNum = 1;
		}
		
		// System.out.println("Misses measured: " + misses);
		// System.out.println("Total GetRequests measured: " + totalGetReqs);
	}

	private void printStatistics(){
		System.out.println("\r\n==============================================================="
			+ "================================================================");
		System.out.println("Final Statistics");
		System.out.println("==============================================================="
			+ "================================================================");

		final Object[] header0 = new String[]{
			"", "", "", "", "Average", "Average", "Average",
			"Average GET", "Average SET", "Average MultiGET"
		};
		final Object[] header1 = new String[]{
			"Timestamp", "GET", "multiGET", "SET", "Throughput",
			"QueueLength", "WaitingTime", 
			"ServiceTime", "ServiceTime", "ServiceTime"
		};
		final Object[] header2 = new String[]{
			"(s)", "", "", "", "(req/s)", "", "("+'\u03BC'+"s)", 
			"("+'\u03BC'+"s)", "("+'\u03BC'+"s)", "("+'\u03BC'+"s)"
		};
		final String[][] table = new String[this.timeStamps.size()][];
		this.measurementTable = table;
		long startTime = this.timeStamps.get(0);
		for (int j=0; j<this.timeStamps.size(); j++) {
			table[j] = new String[]{
				// Convert to seconds with 2 decimal places
				String.valueOf( (float) ( (int) ((this.timeStamps.get(j)-startTime)/10000000L) )/100L),
				String.valueOf(this.getOps.get(j)),
				String.valueOf(this.multigetOps.get(j)),
				String.valueOf(this.setOps.get(j)),
				String.valueOf(this.avgThroughput.get(j)),
				String.valueOf(this.avgQueueLength.get(j)),
				String.valueOf(this.avgQueueWaitTime.get(j)),
				String.valueOf(this.avgGetServiceTime.get(j)),
				String.valueOf(this.avgSetServiceTime.get(j)),
				String.valueOf(this.avgMultigetServiceTime.get(j))
			};
		}
		System.out.format("%-12s%-8s%-10s%-10s%-13s%-13s%-15s%-15s%-15s%-19s\n", header0);
		System.out.format("%-12s%-8s%-10s%-10s%-13s%-13s%-15s%-15s%-15s%-19s\n", header1);
		System.out.format("%-12s%-8s%-10s%-10s%-13s%-13s%-15s%-15s%-15s%-19s\n", header2);
		for (int i=0; i<127; i++) {
			System.out.print("_");
		}
		System.out.println("");
		for( final Object[] row : table) {
			System.out.format("%-12s%-8s%-10s%-10s%-13s%-13s%-15s%-15s%-15s%-19s\n", row);
		}
		System.out.println("");
		System.out.println("Cache miss ratio: " + this.missRatio);

		System.out.println("");
		System.out.println("Average Number of keys in multigets: " + this.avgKeyNum);

	}

	private void printHistogram(){
		System.out.println("");
		System.out.println("==============================================================="
			+ "================================================================");
		System.out.println("Histogram of response times");
		System.out.println("==============================================================="
			+ "================================================================");
		System.out.println("");
		final Object[] header0 = new String[]{"<time (" +'\u03BC'+"s)", "|"
			, "Count", "|", ""};
		System.out.format("%-10s%-1s%-10s%-1s%-1s\n",header0);
		for (int i=0; i<127; i++) {
			System.out.print("_");
		}
		System.out.println("");

		int firstIndex = 0; //((int) this.minResponseTime) / 100; 
		int lastIndex = ((int) this.maxResponseTime) / 100;

		// find max count
		float maxCount = 0;
		for (int i=firstIndex; i<=lastIndex; i++) {
			maxCount = Math.max(maxCount, this.responseTimeHist[i]);
		}
		// System.out.println(firstIndex + " " + lastIndex);
		for (int i=firstIndex; i<=lastIndex; i++) {
			String stars = "";
			System.out.format("%10s%-1s%10s%-1s",
				(i+1)*100,
				"|",
				this.responseTimeHist[i],
				"|"
			);
			for (int j=0; j<this.responseTimeHist[i]/(maxCount/100L); j++) {
				System.out.print("*");
			}
			System.out.print("\n");
		}

	}

	private void printErrors(){
		System.out.println("");
		System.out.println("==============================================================="
			+ "================================================================");
		System.out.println("Errors encountered");
		System.out.println("==============================================================="
			+ "================================================================");
		System.out.println("");
		for (int i=0; i<workers.size(); i++) {
			Worker w = workers.get(i);
			List<String> errors = w.errors;
			for (int j=0; j<errors.size(); j++) {
				System.out.println("Worker error: " + errors.get(j));
			}
		}

		for (int i=0; i<this.netHandler.errors.size(); i++) {
			System.out.println("Net thread error: " + this.netHandler.errors.get(i));
		}
	}

	private void logStatistics(){
		// Create folder for measurements
		try{
			Date now = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String time = dateFormat.format(now);
			String path = System.getProperty("user.dir")+"/Experiments/"+time+"/mw_00/";
			File dir = new File(path);
			boolean dirExists = dir.exists();

			if (!dirExists) {
				Files.createDirectories(Paths.get(path));	
			} else {
				path = System.getProperty("user.dir")+"/Experiments/"+time+"/mw_01/";
				Files.createDirectories(Paths.get(path));
			}

			// Write table to file
			writeTableFile(path);
			writeHistogramFile(path);
			writeStatsFile(path);
			writeErrorFile(path);
		} catch (IOException e) {
			System.out.println("Exception when creating directory");
			e.printStackTrace();
		}

		return;
	}

	private void writeTableFile(String path){
		final String COMMA_DELIMITIER = ",";
		final String NEW_LINE_SEPERATOR = "\n";
		final String FILE_HEADER = 
			"time,get,multiget,set,avgThroughput,avgQueueLength," +
			"avgWaitingTime,avgGetServiceTime,avgSetServiceTime,avgMultigetServiceTime";
		FileWriter fw = null;
		String fileName = "MeasurementTable.csv";
		try {
			fw = new FileWriter(path+fileName);
			fw.append(FILE_HEADER.toString());
			fw.append(NEW_LINE_SEPERATOR);

			for (String[] line : measurementTable) {
				for (String entry : line) {
					fw.append(entry);
					fw.append(COMMA_DELIMITIER);
				}
				fw.append(NEW_LINE_SEPERATOR);
			}
			System.out.println("Measurements written to file: " + path + fileName);
		} catch (IOException e){
			System.out.println("Error while writing to measurementTable csv file.");
			e.printStackTrace();
		} finally {
			try{
				fw.flush();
				fw.close();
			} catch(IOException e){
				System.out.println("Error while flushing or closing measurementTable FileWriter");
				e.printStackTrace();
			}
		}
		return;
	}

	private void writeHistogramFile(String path){
		final String COMMA_DELIMITIER = ",";
		final String NEW_LINE_SEPERATOR = "\n";
		final String FILE_HEADER ="time,requestcount";
		FileWriter fw = null;
		String fileName = "ResponseTimeHistogram.csv";
		try {
			fw = new FileWriter(path+fileName);
			fw.append(FILE_HEADER.toString());
			fw.append(NEW_LINE_SEPERATOR);
			int i = 0;
			int lastIndex = ((int) this.maxResponseTime) / 100;
			for (int count : this.responseTimeHist) {
				i++;
				fw.append(Integer.toString(i*100));
				fw.append(COMMA_DELIMITIER);
				fw.append(Integer.toString(count));
				fw.append(NEW_LINE_SEPERATOR);
				if (i > lastIndex) {
					break;
				}
			}
			System.out.println("Histogram written to file: " + path + fileName);
		} catch (IOException e){
			System.out.println("Error while writing histogram csv file.");
			e.printStackTrace();
		} finally {
			try{
				fw.flush();
				fw.close();
			} catch(IOException e){
				System.out.println("Error while flushing or closing histogram FileWriter");
				e.printStackTrace();
			}
		}
		return;
	}

	private void writeStatsFile(String path){
		final String COMMA_DELIMITIER = ",";
		final String NEW_LINE_SEPERATOR = "\n";
		final String FILE_HEADER = 
			"workerthreads,servers,shardedreads,cachemissratio,avgkeynum";
		FileWriter fw = null;
		String fileName = "statistics.csv";
		try {
			fw = new FileWriter(path+fileName);
			fw.append(FILE_HEADER.toString());
			fw.append(NEW_LINE_SEPERATOR);

			fw.append(Integer.toString(this.n_workers));
			fw.append(COMMA_DELIMITIER);

			fw.append(Integer.toString(this.n_servers));
			fw.append(COMMA_DELIMITIER);

			fw.append(Boolean.toString(this.shardedreads));
			fw.append(COMMA_DELIMITIER);

			fw.append(Float.toString(this.missRatio));
			fw.append(COMMA_DELIMITIER);

			fw.append(Float.toString(this.avgKeyNum));
			fw.append(COMMA_DELIMITIER);
			fw.append(NEW_LINE_SEPERATOR);

			System.out.println("Measurements written to file: " + path + fileName);
		} catch (IOException e){
			System.out.println("Error while writing to stats csv file.");
			e.printStackTrace();
		} finally {
			try{
				fw.flush();
				fw.close();
			} catch(IOException e){
				System.out.println("Error while flushing or closing stats FileWriter");
				e.printStackTrace();
			}
		}
		return;
	}

	private void writeErrorFile(String path){
		FileWriter fw = null;
		String fileName = "errors.log";
		try {
			fw = new FileWriter(path+fileName);

		for (int i=0; i<workers.size(); i++) {
			Worker w = this.workers.get(i);
			List<String> errors = w.errors;
			for (int j=0; j<errors.size(); j++) {
				fw.append("Worker error: " + errors.get(j));
				fw.append("\n");
			}
		}

		for (int i=0; i<this.netHandler.errors.size(); i++) {
			fw.append("Net thread error: " + this.netHandler.errors.get(i));
			fw.append("\n");
		}

			System.out.println("Measurements written to file: " + path + fileName);
		} catch (IOException e){
			System.out.println("Error while writing to error log file.");
			e.printStackTrace();
		} finally {
			try{
				fw.flush();
				fw.close();
			} catch(IOException e){
				System.out.println("Error while flushing or closing error FileWriter");
				e.printStackTrace();
			}
		}
		return;
	}
}