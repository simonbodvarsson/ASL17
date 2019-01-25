import java.nio.channels.SocketChannel;
// Abstract class for SetRequest and GetRequest to extend

abstract public class Request {
	// Full request message
	public String requestString;
	// GET or SET
	public String command;
	// SocketChannel from which the request was sent.
	public SocketChannel sc;
	// System.nanoTime() when the request was put into the request queue
	public long enqueueTime;
}

