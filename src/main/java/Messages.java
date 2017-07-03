import akka.actor.ActorRef;

/**
 * Created by Simo on 05/06/2017.
 */

interface Messages {
}

class Responsibility implements Messages {
    public Double value;
    public int sender;

    public Responsibility (Double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

class Availability implements Messages {
    public Double value;
    public int sender;

    public Availability (Double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

class Neighbors implements Messages {
    public ActorRef[] array;
    public int size;
    public ActorRef aggregator;

    public Neighbors(ActorRef[] array, int size, ActorRef aggregator){
        this.array = array;
        this.size = size;
        this.aggregator = aggregator;
    }
}

class Initialize implements Messages {
    public double[] similarity_row;
    public double[] similarity_col;
    public int selfID;

    public Initialize(double[] similarity_row, double[] similarity_col, int selfID) {
        this.similarity_row = similarity_row;
        this.similarity_col = similarity_col;
        this.selfID = selfID;
    }
}

class Value implements Messages {
    public Double value;
    public int sender;
    public long iteration;

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