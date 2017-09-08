package affinityPropagation;

import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

/**
 * Simple class for managing a Timer.
 * Use org.joda.time Library.
 *
 * @author Simone Schirinzi
 */
class Timer{

    /** Instant to start counting */
    private long startTime;

    /** Instant to stop counting */
    private long endTime;

    /** Interval Printing Mode */
    private final PeriodFormatter fmt;

    /**
     * Initialize the timer by printing time in the format : "D h m:s.ms"
     * */
    Timer(){
        fmt = new PeriodFormatterBuilder()
                .printZeroAlways()
                .minimumPrintedDigits(1)
                .appendDays()
                .appendLiteral("D ")
                .appendHours()
                .appendLiteral("h ")
                .minimumPrintedDigits(2)
                .appendMinutes()
                .appendSeparator(":")
                .appendSecondsWithMillis()
                .toFormatter();
    }

    /**
     * Start the stopwatch, setting startTime to the current milliseconds.
     * */
    void start(){
        this.startTime = System.currentTimeMillis();
    }

    /**
     * End the stopwatch, setting endTime to the current milliseconds.
     * */
    void stop(){
        this.endTime = System.currentTimeMillis();
    }

    /**
     * Get the time elapsed.
     * @return string represent date
     * */
    private String getDate(){

        /* Converts two time instants to a value in milliseconds, representing how long it has elapsed. */
        Duration timer = new Duration(this.startTime,this.endTime);

		/* Converts the milliseconds into a formatted period in different fields: years, hours, seconds, etc ... */
        Period p = timer.toPeriod();

		/* Converts the period in a string, as decided in initialization */
        return fmt.print(p);
    }

    /**
     * @return string represent date
     */
    @Override
    public String toString(){
        return getDate();
    }
}
