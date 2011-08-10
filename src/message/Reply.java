package message;

import userDatabase.User;

public interface Reply extends BasicMessage {

	User getChannel();

}
