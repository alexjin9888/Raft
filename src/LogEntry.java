public class LogEntry {
    String command;
    int term;

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
