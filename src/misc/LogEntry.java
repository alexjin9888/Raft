package misc;

/**
 * Each instance of LogEntry class corresponds to a log.
 * Proj2: figure out what to do with command
 *
 */
public class LogEntry {
    /**
     * See RAFT figure 2 for explanation of these variables
     */
    public String command;
    public int term;

    /**
     * @param command To be executed on each server
     * @param term    term of the leader when it sent this log
     */
    public LogEntry(String command, int term) {
        super();
        this.command = command;
        this.term = term;
    }

    @Override
    public String toString() {
        return "LogEntry [command=" + command + ", term=" + term + "]";
    }
}
