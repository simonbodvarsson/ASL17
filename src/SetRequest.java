import java.nio.channels.SocketChannel;
// Wrapper class for SET requests.

public class SetRequest extends Request {
	// The data
	public String data;
	// The key
	public String key;
	// Length of data
	public int bytes;

	// Constructor
	SetRequest(String requestString, String command,
						String key, SocketChannel sc, 
						int bytes,	String data) {
		this.requestString = requestString;
		this.command = command;
		this.key = key;
		this.sc = sc;
		this.bytes = bytes;
		this.data = data;
		this.enqueueTime = System.nanoTime();
	}
}