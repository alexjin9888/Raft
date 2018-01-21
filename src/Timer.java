import java.time.Instant;
import java.util.Date;

public class Timer {
    private Date lastTimeoutTime;
    private int timeoutInterval;

    public Timer() {
        super();
        this.lastTimeoutTime = null;
        this.timeoutInterval = 0;
    }

    public void reset(int timeoutInterval) {
        this.lastTimeoutTime = Date.from(Instant.now());
        this.timeoutInterval = timeoutInterval;
    }
    
    public boolean timeIsUp() {
        if (lastTimeoutTime == null) {
            return true;
        }
        return Date.from(Instant.now()).getTime() - lastTimeoutTime.getTime() >= timeoutInterval;
    }
}
