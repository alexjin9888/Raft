package singletons;
import java.time.Instant;
import java.util.Date;

/**
 * An instance of this class functions as a timer.
 */
public class Timer {
    private Date lastTimeoutTime; // last absolute reset time
    private int timeoutInterval; // user-specified timeout interval

    public Timer() {
        super();
        this.lastTimeoutTime = null;
        this.timeoutInterval = 0;
    }

    /**
     * Start or restarts the timer by specifying the timeout interval.
     * @param timeoutInterval How long we want the timer to go for (in ms)
     */
    public void reset(int timeoutInterval) {
        this.lastTimeoutTime = Date.from(Instant.now());
        this.timeoutInterval = timeoutInterval;
    }
    
    /** 
     * Tells you whether the timer has finished counting down.
     * @return true iff the timer has finished counting down.
     */
    public boolean timeIsUp() {
        if (lastTimeoutTime == null) {
            return true;
        }
        return Date.from(Instant.now()).getTime() - 
            lastTimeoutTime.getTime() >= timeoutInterval;
    }
}
