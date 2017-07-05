import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

/**
 * Semplice classe per la gestione di un Timer.
 * Sfrutta la org.joda.time Library.
 *
 * @author Simone Schirinzi
 */
class Timer{

    /** Istante da cui iniziare a contare */
    private long startTime;

    /** Istante in cui finire di contare */
    private long endTime;

    /** Modalità di stampa dell'intervallo */
    private final PeriodFormatter fmt;

    /** Inizializza il timer stampando il tempo nel formato: "D h m:s.ms" */
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
     * Avvia il cronometro, settando startTime ai millisecondi correnti.
     *
     * */
    public void Start(){
        this.startTime = System.currentTimeMillis();
    }

    /** Termina il cronometro, settando endTime ai millisecondi correnti */
    public void Stop(){
        this.endTime = System.currentTimeMillis();
    }

    /** Ottengo il periodo trascorso */
    private String getDate(){

		/* Converte i due istanti di tempo in un valore, in millisecondi,
		 * rappresentante quanto tempo � trascorso. */
        Duration timer = new Duration(this.startTime,this.endTime);

		/*Converte i millisecondi in un un periodo formattato in diversi campi: anni, ore, secondi, ecc...*/
        Period p = timer.toPeriod();

		/* Converto il perido in una stringa, secondo come deciso
		 * in fase di inizializzazione
		 */
        return fmt.print(p);
    }

    /** @see this.getDate
     *
     */
    @Override
    public String toString(){
        return getDate();
    }
}
