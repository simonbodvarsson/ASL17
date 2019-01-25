import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.*;
import java.util.concurrent.*;

// NetHandler accepts new sockets and checks for messages from clients. 
// When ready requests are found, they are added to the request queue for
// workers to handle.
public class NetHandler implements Runnable {
			
	private int port = 0;
	private Selector selector = null;
	public BlockingQueue<Request> requestQueue = null;
	public List<String> errors = new ArrayList<>();


	NetHandler(int myPort, BlockingQueue<Request> requestQueue) {
		this.port = myPort;
		this.requestQueue = requestQueue;
		this.selector = null;
	}

	public void run() {
		try{
			// Create selector
			this.selector = Selector.open();
			// Create ServerSocketChannel		
			ServerSocketChannel serverSocket = ServerSocketChannel.open();
			serverSocket.configureBlocking(false);
			serverSocket.bind(new InetSocketAddress(port));
			// Register ServerSocketChannel to selector
			SelectionKey ssckey = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		
		} catch(IOException e) {
			e.printStackTrace();
			this.errors.add("Thread " + Thread.currentThread().getName() 
             	+ " " + e.getMessage());
			return;
		}

		while(true) {
			try{ 
				// Select ready channels
				int readyChannels = selector.select();
				if(readyChannels == 0) continue;

				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

				while(keyIterator.hasNext()) {
					SelectionKey key = keyIterator.next();

					if (key.isAcceptable()) {
						// accept a new connection to serverSocketChannel
						ServerSocketChannel ssChannel = (ServerSocketChannel) key.channel();
						SocketChannel client = ssChannel.accept();
						client.configureBlocking(false);
						// Register this channel to the selector and add the charBuffer for
						// that channel.
						ByteBuffer buf = ByteBuffer.allocate(2048); // What should this be???
						SelectionKey skey = client.register(selector,
											SelectionKey.OP_READ, buf);
						//System.out.println("New client: " + client);
					} else if (key.isReadable()) {
						// A channel is ready for reading
						// Add the msg to the Bytebuffer for this channel...
						ByteBuffer buf = (ByteBuffer) key.attachment();
						SocketChannel sc = (SocketChannel) key.channel();

						int bytesRead = sc.read(buf);
						while(bytesRead > 0){
							//System.out.println("Read " + bytesRead + " bytes.");
							// Check if buffer contains full request
							// if it does, add it to queue
							this.CheckEnqueueRequest(buf, sc);

							bytesRead = sc.read(buf);
						}
					}

					keyIterator.remove();
				}
			} catch(IOException e) {
				this.errors.add("Thread " + Thread.currentThread().getName() 
             	+ " " + e.getMessage());
				e.printStackTrace();
			}
		}
	}


	// Check if buffer has a complete request and add it to the queue along with 
	// the socketChannel to the client.
	private boolean CheckEnqueueRequest(ByteBuffer buf, SocketChannel sc) {
		// Make buffer readable
		buf.flip();
		char[] charArray = new char[buf.remaining()+1];

		int j = 0;
		while(buf.hasRemaining()) {
			charArray[j] = (char) buf.get();
			j++;
		}

		String msg = new String(charArray);
		String com = msg.substring(0,3);
		
		int endRequestIndex;
		String[] msgArr;

		if( com.equals("get")){
			// find whole request: <com> <key> ... <key> \r\n
			endRequestIndex = msg.indexOf("\r\n");
			if(endRequestIndex == -1){
				// End of line not found. Msg is incomplete.
				System.out.println("Incomplete request.");
				buf.rewind();
				return false;
			} else {
				int totalMsgLength = endRequestIndex + 2;
				String requestString = msg.substring(0,totalMsgLength);
				String[] keys = requestString.substring(4).split(" ");

				Request request = new GetRequest(requestString, com, keys, sc);
				//request.enqueueTime = System.currentTimeMillis(); //System.nanoTime();
				this.requestQueue.add(request);
				
				// Fix buffer to make unused bytes readable again.
				fixBuffer(totalMsgLength, buf);

				return true;
			}

		} else if( com.equals("set")){
			try{
				// find all parameters: <com> <key> <flags> <exptime> <bytes> \r\n
				int firstLineIndex = msg.indexOf("\r\n");
				if(firstLineIndex == -1){
					// End of line not found. Msg is incomplete
					System.out.println("Incomplete request.");
					buf.rewind();
					buf.compact();
					return false;
				}

				String[] paramArr = msg.substring(0,firstLineIndex).split(" ");
				
				String key = paramArr[1];
				int bytes = Integer.parseInt(paramArr[4]);

				// System.out.println("params: " +  params);	
				// find data block: <data block> \r\n
				if (msg.length() < firstLineIndex+4+bytes) {
					// Data block is incomplete.
					System.out.println("Incomplete data block.");
					buf.rewind();
					buf.compact();
					return false;
				}

				String data = msg.substring(firstLineIndex+2, firstLineIndex+2+bytes);
				int totalMsgLength = firstLineIndex+bytes+4;
				String requestString = msg.substring(0,totalMsgLength);

				Request request = new SetRequest(requestString, com, key, sc, bytes, data);

				this.requestQueue.add(request);
				
				// Fix buffer to make unused bytes readable again.
				fixBuffer(totalMsgLength,buf);

				return true;

			} catch (Exception e){
				//System.out.println(e);
				//System.out.println("Incomplete request.");
				buf.rewind();
				buf.compact();
				this.errors.add(" Request not understood:" + e.getMessage());
				return false;
			}
		} else {
			// Unknown type of request.
			int endLineIndex = msg.indexOf("\r\n");
			if(endLineIndex == -1){
				// End of line not found. Msg is incomplete
				//System.out.println("Incomplete request.");
				buf.rewind();
				buf.compact();
				return false;
			}
			// Discard data until next newline.
			fixBuffer(endLineIndex+2, buf);
			this.errors.add(" Request not understood: " + msg );
		}
		buf.rewind();
		buf.compact();
		return false;
	}


	private void fixBuffer(int totalMsgLength, ByteBuffer buf){
		buf.rewind();
		for(int i = 0; i<totalMsgLength; i++){
			buf.get();
		}
		buf.compact();
		return;
	}
	

	public static int ordinalIndexOf(String str, String substr, int n) {
	    int pos = str.indexOf(substr);
	    while (--n > 0 && pos != -1)
        	pos = str.indexOf(substr, pos + 1);
    	return pos;
	}
}