package misc;

/**
 * Each instance of LogEntry class corresponds to a log.
 * Proj2: figure out what to do with command
 */
public class LogEntry {
    // To be executed on each server
    public String command;
    // term of the leader when it sent this log
    public int term;

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
