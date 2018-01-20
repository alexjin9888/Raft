import java.io.Serializable;

/*
 * This is an RPC message that is sent and received among servers.
 */
public abstract class Message implements Serializable {
	// serverId is candidateId in RequestVote RPC
	//             leaderId in AppendEntries RPC
    String serverId;
    int term; // currentTerm of the sender
}
