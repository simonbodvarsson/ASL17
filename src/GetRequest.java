import java.nio.channels.SocketChannel;
// Wrapper class for GET requests

public class GetRequest extends Request {
	// Number of different keys requested (if keyNum > 1, then this is a multiget)
	public int keyNum;
	// The requested keys
	public String[] keys;

	// Constructor
	GetRequest(String requestString, String command,
						String[] keys, SocketChannel sc){
		this.requestString = requestString;
		this.command = command;
		this.keys = keys;
		this.keyNum = keys.length;
		this.sc = sc;
		this.enqueueTime = System.nanoTime();
	}
}