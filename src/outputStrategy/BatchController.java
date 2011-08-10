package outputStrategy;

import internalInformationPort.InternalInformationPortController;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import message.BatchSizeMessage;
import message.ChannelEstablishMessage;
import message.ChannelMessage;
import message.Message;
import message.Reply;
import message.Request;
import userDatabase.User;
import architectureInterface.OutputStrategyInterface;

public class BatchController implements OutputStrategyInterface {

	/**
	 * Data structure used to stores processed messages until output is requested 
	 * (see <code>putOutBatch()</code>). Then, all messages in the buffer are 
	 * submitted to the <code>InputOutputHandler</code>, which sends them to their 
	 * destination. Messages are added in sorted manner (alphabetic, ascending 
	 * order) to prevent linkability of (incoming and outgoing) messages due to 
	 * their position in the input and output stream.
	 * <p>
	 * This class is thread-safe.
	 * 
	 * @author Karl-Peter Fuchs
	 */
	final class Batch {

		/**
		 * Indicates whether this <code>Batch</code> is used to collect 
		 * <code>Request</code>s or <code>Reply</code>ies.
		 */
		private final boolean IS_REQUEST_BATCH;

		/**
		 * Indicates whether this <code>Batch</code> belongs to the last mix of a 
		 * cascade or not.
		 */
		private final boolean BELONGS_TO_LAST_MIX;

		/** ArrayList containing the messages. */
		private ArrayList<Message> buffer;

		/**
		 * Position, the message currently processed shall be saved to (in 
		 * <code>buffer</code>). <code>Calculated by findCorrectPosition(Message 
		 * message, int startIndex, int endIndex)</code>.
		 * 
		 * @see #buffer
		 * @see #findCorrectPosition(Message, int, int)
		 */
		private int correctPosition;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
		//todo: RŸckgabe an den InputOutputHandler ermšglichen
=======
>>>>>>> added Eclipse project
=======
		//todo: RŸckgabe an den InputOutputHandler ermšglichen
>>>>>>> Minor changes on BatchController
=======
=======
>>>>>>> 0494672f99d1fc56636325d4e75aeda461de1a5b
		//todo: RŸckgabe an den InputOutputHandler ermšglichen
=======
>>>>>>> added Eclipse project
>>>>>>> Minor changed on BatchController
=======
>>>>>>> added Eclipse project
=======
		//todo: RŸckgabe an den InputOutputHandler ermšglichen
>>>>>>> Minor changes on BatchController
=======
		//todo: RŸckgabe an den InputOutputHandler ermšglichen
=======
>>>>>>> added Eclipse project
>>>>>>> Minor changed on BatchController
=======
		//todo: RŸckgabe an den InputOutputHandler ermšglichen
=======
>>>>>>> added Eclipse project
>>>>>>> 0f7eff4f5b03d5e6b3c5911e446dacd41f64cfa7
		/**
		 * Reference on <code>InputOutputHandler</code> used to send messages to 
		 * their destination when <code>putOutBatch()</code> was called.
		 */
		private InputOutputHandlerController inputOutputHandler;


		/**
		 * Constructs a new <code>Batch</code> that can be used to store processed 
		 * messages until output is requested (see <code>putOutBatch()</code>). 
		 * 
		 * @param initialMessageBufferSize		Initial size for the data structure 
		 * 										(<code>ArrayList</code>) used to 
		 * 										store messages.
		 * @param isRequestBatch				Indicates whether this 
		 * 										<code>Batch</code> is used to 
		 * 										collect <code>Request</code>s or 
		 * 										<code>Reply</code>ies.
		 * @param belongsToLastMix				Indicates whether this 
		 * 										<code>Batch</code> belongs to the 
		 * 										last mix of a cascade or not.
		 * @param inputOutputHandlerController	Reference on component
		 * 										<code>InputOutputHandler</code> 
		 * 										(used to send messages to their 
		 * 										destination when 
		 * 										<code>putOutBatch()</code> was 
		 * 										called).
		 */
		protected Batch(
				int initialMessageBufferSize,
				boolean isRequestBatch,
				boolean belongsToLastMix,
				InputOutputHandlerController inputOutputHandlerController
		) {

			this.IS_REQUEST_BATCH = isRequestBatch;
			this.BELONGS_TO_LAST_MIX = belongsToLastMix;
			this.buffer = new ArrayList<Message>(initialMessageBufferSize);
			this.inputOutputHandler = inputOutputHandlerController;

		}


		/**
		 * Returns the total number of messages currently in this 
		 * <code>Batch</code>.
		 * 
		 * @return	Total number of messages currently in this <code>Batch</code>.
		 */
		protected synchronized int size() {

			return buffer.size();

		}


		/**
		 * Adds the bypassed message to the local buffer (at the correct position 
		 * according to alphabetic, ascending order).
		 * 
		 * @param message Message to be added to local buffer.
		 */
		protected synchronized void addMessage(Message message) {

			findCorrectPosition(message, 0, (buffer.size() - 1));
			buffer.add(correctPosition, message);

		}


		/**
		 * Finds the correct position (alphabetic, ascending order) the bypassed 
		 * message shall be saved to (in <code>buffer</code>). (Recursive; 
		 * divide and conquer).
		 * 
		 * @see #correctPosition
		 * @see #buffer
		 * @see Message#compareTo(Message)
		 */
		private void findCorrectPosition(	Message message, 
				int startIndex, 
				int endIndex
		) {

			if (buffer.size() == 0) { // first message

				correctPosition = 0;

			} else {

				if (startIndex <= endIndex) {

					int mid = (startIndex + endIndex) / 2;

					switch (message.compareTo(buffer.get(mid))) {

					case -1: // bypassed message is smaller

						correctPosition = mid;
						findCorrectPosition(message, startIndex, mid - 1);
						break;					

					case  0: // messages are equal

						correctPosition = mid;
						startIndex = endIndex; // stop execution
						break;						

					case  1: // bypassed message is larger

						correctPosition = mid + 1;
						findCorrectPosition(message, mid + 1, endIndex);
						break;

					}

				}

			}

		}


		/**
		 * Puts out the current batch, by submitting all messages in buffer to 
		 * the <code>InputOutputHandler</code>, which sends them to their 
		 * destination.
		 */
		protected synchronized void putOutBatch() {

			if (BELONGS_TO_LAST_MIX && IS_REQUEST_BATCH) {

				inputOutputHandler.addRequests(buffer.toArray(new Request[0]));

			} else {

				for (int i=0; i<buffer.size(); i++) {

					Message message = buffer.get(i);
					User channel = message.getChannel();

					if (IS_REQUEST_BATCH) {

						inputOutputHandler.addRequest(
								(Request) message
						);

						// allow client to add a new message (for next batch)
						channel.setHasMessageInCurrentBatch(false);

					} else {

						inputOutputHandler.addReply((Reply) message);


						// allow client to add a new message (for next batch)
						channel.setHasMessageInCurrentReplyBatch(false);

					}

				}

			}

			buffer.clear();

		}

	}


	/** Data structure used to store requests before output. */
	private Batch requestBatch;

	/** Data structure used to store replies before output. */
	private Batch replyBatch;

	/** 
	 * Reference on component <code>InternalInformationPort</code>. 
	 * Used to display and/or log data and read general settings.
	 */
	private static InternalInformationPortController internalInformationPort = 
		new InternalInformationPortController();

	/**
	 * Initial size of the <code>Buffer</code> used to store messages until 
	 * output. Gets resized automatically if it runs out of space.
	 */
	private final int INITIAL_BUFFER_SIZE;

	/**
	 * Indicates whether this <code>OutputStrategyController</code> belongs to 
	 * the last mix of the cascade or not.
	 */
	private boolean BELONGS_TO_LAST_MIX;

	/**
	 * Indicates whether this <code>OutputStrategyController</code> belongs to 
	 * the first mix of the cascade or not.
	 */
	private boolean BELONGS_TO_FIRST_MIX;

	/**
	 * Amount of time, after which the batch is put out, no matter how many 
	 * messages it contains.
	 */
	private final long TIMEOUT;

	/** Timer used to detect <code>TIMEOUT</code> for requests. */
	private Timer requestTimeoutTimer = new Timer();

	/** Timer used to detect <code>TIMEOUT</code> for replies. */
	private Timer replyTimeoutTimer = new Timer();

	/** Logger used to log and display information. */
	private final static Logger LOGGER = internalInformationPort.getLogger();


	/**
	 * Minimum number of <code>ReplyMessages</code>s that must be collected, 
	 * before putting out the reply batch. Will be adjusted 
	 * automatically.
	 * 
	 * @see #replyBatch
	 */
	private int neededReplyMessages = 0; // will be set dynamically

	/** 
	 * Indicates whether the batch sent lastly has already been answered.
	 * <p>
	 * Note: Synchronous batch.
	 */
	private boolean isReplyBatchPending = false;

	/** 
	 * Number of messages the upcoming batch will contain (according to the 
	 * <code>OutputStrategy</code> component on this mix' predecessor).
	 * <p>
	 * Used for batch synchronization.
	 * 
	 * @see message.BatchSizeMessage
	 */
	private int batchSize;

	/**
	 * Minimum number of <code>ChannelEstablishMessage</code>s that must be 
	 * collected, before putting out the request batch (if at least one 
	 * <code>ChannelEstablishMessage</code> is in the batch).
	 * 
	 * @see #requestBatch
	 */
	private int neededChannelEstablishMessages;

	/**
	 * Minimum number of <code>ForwardChannelMessage</code>s that must be 
	 * collected, before putting out the request batch. Will be adjusted 
	 * automatically.
	 * 
	 * @see #requestBatch
	 */
	private int neededForwardChannelMessages = 0;

	/**
	 * Number of <code>ChannelEstablishMessage</code>s currently in request 
	 * batch.
	 * 
	 * @see #neededChannelEstablishMessages
	 */
	private int numberOfChannelEstablishMessages = 0;

	/**
	 * Number of <code>ForwardChannelMessage</code>s currently in request 
	 * batch.
	 * 
	 * @see #neededForwardChannelMessages
	 */
	private int numberOfForwardChannelMessages = 0;

	/**
	 * Number of <code>ChannelReleaseMessage</code>s currently in request 
	 * batch.
	 */
	private int numberOfChannelReleaseMessages = 0;

	/**
	 * Generates a new <code>OutputStrategy</code> component, which collects 
	 * messages until an output criterion is fulfilled (certain number of 
	 * messages collected or timeout reached).
	 * <p>
	 * Messages are added by component <code>MessageProcessor</code>. When the 
	 * output criterion is fulfilled, the collected messages are bypassed to 
	 * the <code>InputOutputHandler</code> (component), which sends them to 
	 * their destination.
	 * <p>
	 * Can handle <code>Request</code>s and <code>Replies</code> in parallel.
	 * <p>
	 * Component can't be used before calling 
	 * <code>initialize(BatchController)</code>.
	 * 
	 * @see #initialize(BatchController)
	 */
	public BatchController() {

		this.BELONGS_TO_LAST_MIX = 
			(new Integer(getProperty("NUMBER_OF_FURTHER_MIXES")) == 0)
			? true 
					: false;

		this.BELONGS_TO_FIRST_MIX = 
			(new Integer(getProperty("NUMBER_OF_PREVIOUS_MIXES")) == 0)
			? true 
					: false;

		this.INITIAL_BUFFER_SIZE = 
			new Integer(getProperty("INITIAL_BUFFER_SIZE"));

		this.neededChannelEstablishMessages = 
			new Integer(getProperty("NEEDED_CHANNEL_ESTABLISH_MESSAGES"));

		this.TIMEOUT = new Long(getProperty("BATCH_TIMEOUT"));

	}


	/**
	 * Initialization method for this component. Makes this component ready 
	 * for accepting messages.
	 * 
	 * @param inputOutputHandler	Reference on component 
	 * 								<code>InputOutputHandler</code> (used to 
	 * 								send messages after output).
	 */
	public void initialize() {

		LOGGER.fine("Batchcontroller... initializing");

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Minor changed on BatchController
=======
>>>>>>> 0494672f99d1fc56636325d4e75aeda461de1a5b
=======
>>>>>>> Minor changed on BatchController
=======
>>>>>>> 0f7eff4f5b03d5e6b3c5911e446dacd41f64cfa7
=======


>>>>>>> added Eclipse project
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Minor changes on BatchController
=======
>>>>>>> Minor changed on BatchController
=======


>>>>>>> added Eclipse project
=======
>>>>>>> 0494672f99d1fc56636325d4e75aeda461de1a5b
=======
>>>>>>> Minor changes on BatchController
=======
>>>>>>> Minor changed on BatchController
=======
>>>>>>> 0f7eff4f5b03d5e6b3c5911e446dacd41f64cfa7
		this.requestBatch = 
			new Batch(	INITIAL_BUFFER_SIZE, 
					true,
					BELONGS_TO_LAST_MIX,
					inputOutputHandler
			);

		this.replyBatch = 
			new Batch(	INITIAL_BUFFER_SIZE, 
					false,
					BELONGS_TO_LAST_MIX,
					inputOutputHandler
			);

	}


	/**
	 * Can be used to add a <code>Request</code>, that shall be put out 
	 * according to the underlying output strategy.
	 * <p>
	 * Return immediately (asynchronous behavior), internal output 
	 * decision is deferred.
	 * 
	 * @param request	<code>Request</code>, that shall be put out according 
	 * 					to the underlying output strategy.
	 * 					A special <code>Request</code> is the <code>
	 * 					BatchSizeMessage</code>, which contains the number of
	 * 					messages of the upcoming batch (according to the
	 * 					mix' predecessor). This message is used for batch
	 * 					synchronization.
	 * 
	 * @see message.BatchSizeMessage
	 */
	@Override
	public void addRequest(Request request) {

		if (request instanceof BatchSizeMessage)

		{
			this.batchSize =  ((BatchSizeMessage) request).getBatchSize();
		}

		else

		{

			synchronized (requestBatch) {

				User channel = request.getChannel();

				// indicate that a message for this channel has been added to the 
				// current batch
				channel.setHasMessageInCurrentBatch(true);
				requestBatch.addMessage((Message)request);

				// increment suiting message-counter
				if (request instanceof ChannelEstablishMessage) {

					numberOfChannelEstablishMessages++;

				} else if (request instanceof ChannelMessage) {

					numberOfForwardChannelMessages++;

				} else { // ChannelReleaseMessage

					numberOfChannelReleaseMessages++;
					neededForwardChannelMessages--;

				}

				if (requestBatch.size() == 1) { // first message of batch

					requestTimeoutTimer = new Timer();

					requestTimeoutTimer.schedule(	new BatchOutputTask(requestBatch), 
							TIMEOUT
					);

				}

				if (isOutputCriterionForRequestBatchFulfilled()) {

					requestTimeoutTimer.cancel();
					putOutRequestBatch();

				}

			}

		}

	}

	@Override
	public void addReply(Reply reply) {

		synchronized (replyBatch) {

			User channel = reply.getChannel();
			isReplyBatchPending = false;

			// indicate that a message for this channel has been added to the 
			// current batch
			channel.setHasMessageInCurrentReplyBatch(true);
			replyBatch.addMessage((Message) reply);

			if (replyBatch.size() == 1) { // first message of batch

				replyTimeoutTimer = new Timer();

				replyTimeoutTimer.schedule(	new BatchOutputTask(replyBatch), 
						TIMEOUT
				);

			}

			if (isOutputCriterionForReplyBatchFulfilled()) {

				replyTimeoutTimer.cancel();
				putOutReplyBatch();

			}

		}

	}


	/**
	 * Indicates whether the output criterion for <code>requestBatch</code> is 
	 * fulfilled or not.
	 * 
	 * @return	Whether the output criterion for <code>requestBatch</code> is 
	 * 			fulfilled or not.
	 */
	private boolean isOutputCriterionForRequestBatchFulfilled() {

		if (requestBatch.size() == 0) {

			return false;

		} else {

			if (BELONGS_TO_FIRST_MIX) {

				boolean enoughChannelEstablishMessages = 
					(	numberOfChannelEstablishMessages == 0
							||
							numberOfChannelEstablishMessages >= 
								neededChannelEstablishMessages)
								? true 
										: false;

				boolean enoughForwardChannelMessages = 
					(	numberOfForwardChannelMessages >= 
						neededForwardChannelMessages)
						? true 
								: false;

				return (	!isReplyBatchPending
						&&
						enoughChannelEstablishMessages
						&&
						enoughForwardChannelMessages
				);

			} else {

				return (	numberOfChannelEstablishMessages 
						+ 
						numberOfForwardChannelMessages
						+ 
						numberOfChannelReleaseMessages

				) == batchSize;

			}

		}

	}


	/**
	 * Puts out collected messages in <code>replyBatch</code>.
	 */
	private void putOutReplyBatch() {

		synchronized (replyBatch) {

			replyBatch.putOutBatch();

		}

	}

	/**
	 * Indicates whether the output criterion for <code>replyBatch</code> is 
	 * fulfilled or not.
	 * 
	 * @return	Whether the output criterion for <code>replyBatch</code> is 
	 * 			fulfilled or not.
	 */
	private boolean isOutputCriterionForReplyBatchFulfilled() {

		if (replyBatch.size() == 0) {

			return false;

		} else {

			return (replyBatch.size() >= neededReplyMessages);

		}

	}


	/**
	 * Puts out collected messages in <code>requestBatch</code> and prepares 
	 * variables for next batch.
	 */
	private void putOutRequestBatch() {

		synchronized (requestBatch) {

			if (!BELONGS_TO_LAST_MIX) {
				// send BatchSizeMessage to next mix for batch synchronization

				int batchSizeForNextMix = 
					numberOfForwardChannelMessages
					+ numberOfChannelEstablishMessages
					+ numberOfChannelReleaseMessages;

				BatchSizeMessage batchSizeMessage = 
					new BatchSizeMessage(batchSizeForNextMix);

				inputOutputHandler.addRequest(batchSizeMessage);

			}

			if (BELONGS_TO_FIRST_MIX) {

				// calculate (expected) number of messages for next batch
				neededForwardChannelMessages = 
					neededForwardChannelMessages
					+ numberOfChannelEstablishMessages
					- numberOfChannelReleaseMessages;

				neededReplyMessages = neededForwardChannelMessages;

			} else { // not first mix

				neededReplyMessages = 
					batchSize - numberOfChannelReleaseMessages;

			}

			// reset message counters
			numberOfChannelEstablishMessages = 0;
			numberOfForwardChannelMessages = 0;
			numberOfChannelReleaseMessages = 0;

			isReplyBatchPending = true;

			requestBatch.putOutBatch();

		}

	}



	/**
	 * Simply used to shorten method calls (calls 
	 * <code>internalInformationPort.getProperty(key)</code>). Returns the 
	 * property with the specified key from the property file.
	 * 
	 * @param key	The property key.
	 * 
	 * @return		The property with the specified key in the property file.
	 */
	private static String getProperty(String key) {

		return internalInformationPort.getProperty(key);

	}


	/**
	 * Simple <code>TimerTask</code>, which puts out the batch it is linked to.
	 * 
	 * @author Karl-Peter Fuchs
	 */
	final class BatchOutputTask extends TimerTask {

		/**
		 * Indicates whether this <code>OutputTask</code> is linked with 
		 * <code>requestBatch</code> or not (= linked with 
		 * <code>replyBatch</code>).
		 */
		private boolean isRequestTimer;


		/**
		 * Creates a new <code>OutputTask</code> for the specified 
		 * <code>Batch</code>.
		 * 
		 * @param batch	<code>Batch</code> that shall be put out.
		 */
		protected BatchOutputTask(Batch batch) {

			isRequestTimer = (batch == requestBatch) ? true : false;

		}


		/**
		 * Puts out the batch it is linked to.
		 */
		@Override 
		public void run() {

			if (isRequestTimer) {

				LOGGER.fine("(MessageBuffer) Request-Timeout reached!");
				requestTimeoutTimer.cancel();
				putOutRequestBatch();

			} else {

				LOGGER.fine("(MessageBuffer) Reply-Timeout reached!");
				replyTimeoutTimer.cancel();
				putOutReplyBatch();

			}

		}

	}

}
