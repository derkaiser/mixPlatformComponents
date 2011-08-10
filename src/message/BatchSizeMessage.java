package message;

import userDatabase.User;

public class BatchSizeMessage extends Message implements Request, 
InternalMessage {

	public BatchSizeMessage(int batchSizeForNextMix) {
		// TODO Auto-generated constructor stub
	}

	public int getBatchSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public User getChannel() {
		// TODO Auto-generated method stub
		return null;
	}

}
