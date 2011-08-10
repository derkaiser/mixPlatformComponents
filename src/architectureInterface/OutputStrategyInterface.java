package architectureInterface;


import message.Reply;
import message.Request;


/**
 * Architecture interface for component <code>OutputStrategy</code>. 
 * <p>
 * Must accept messages (<code>Request</code>s and <code>Reply</code>ies) from 
 * component <code>MessageProcessor</code> and put them out (hand them over to 
 * component <code>InputOutputHandler</code>, which sends them to their 
 * destination) according to an underlying strategy (e. g. batch strategy).
 * <p>
 * Must be thread-safe.
 * 
 * @author Karl-Peter Fuchs
 */
public interface OutputStrategyInterface {

	
	/**
	 * Can be used to add a <code>Request</code>, that shall be put out 
	 * according to the underlying output strategy.
	 * <p>
	 * Must return immediately (asynchronous behavior), internal output 
	 * decision may be deferred.
	 * 
	 * @param request	<code>Request</code>, that shall be put out according 
	 * 					to the underlying output strategy.
	 */
	public void addRequest(Request request);
	
	
	/**
	 * Can be used to add a <code>Reply</code>, that shall be put out 
	 * according to the underlying output strategy.
	 * <p>
	 * Must return immediately (asynchronous behavior), internal output 
	 * decision may be deferred.
	 * 
	 * @param reply	<code>Reply</code>, that shall be put out according 
	 * 				to the underlying output strategy.
	 */
	public void addReply(Reply reply);

	/**
	 * Initialize the component...
	 */
	public void initialize();
	
}
