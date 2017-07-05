import akka.actor.ActorRef;

/**
 * Created by Simo on 05/06/2017.
 */

@SuppressWarnings("DefaultFileTemplate")
interface Messages {
}

class Responsibility implements Messages {
    public final double value;
    public final int sender;

    public Responsibility (double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

class Availability implements Messages {
    public final double value;
    public final int sender;

    public Availability (double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

class Neighbors implements Messages {
    public final ActorRef[] array;
    public final int size;
    public final ActorRef aggregator;

    public Neighbors(ActorRef[] array, int size, ActorRef aggregator){
        this.array = array;
        this.size = size;
        this.aggregator = aggregator;
    }
}

class Initialize implements Messages {
    public final double[] similarity_row;
    public final double[] similarity_col;
    public final int selfID;

    public Initialize(double[] similarity_row, double[] similarity_col, int selfID) {
        this.similarity_row = similarity_row;
        this.similarity_col = similarity_col;
        this.selfID = selfID;
    }
}

class Value implements Messages {
    public final Double value;
    public final int sender;
    public final long iteration;

    public Value(Double value, int sender, long iteration){
        this.value = value;
        this.sender = sender;
        this.iteration = iteration;
    }
}

class Self implements Messages {

}

class Die implements Messages{

}

class Start implements Messages {

}

class Ready implements Messages {

}