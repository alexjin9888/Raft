import java.io.Serializable;

/*
 * This is an RPC message that is sent and received among servers.
 */
public abstract class Message implements Serializable {
    String serverId;
}
