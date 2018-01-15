/*
 * Reply to AppendEntriesRPCMessages   
 */
public class AppendEntriesReply extends Message {
    int term;           // currentTerm, for leader to update itself
    boolean success;    // true if  follower contained entry matching
                        // prevLogIndex and prevLogTerm
}
