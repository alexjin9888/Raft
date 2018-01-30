package units;

/**
 * A log entry in a server's command log.
 * Proj2: figure out what to do with command string
 */
public class LogEntry {
    public String command; // To be executed on each server
    public int term; // term of the leader when it sent this log

    /**
     * @param command see above
     * @param term    see above
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
