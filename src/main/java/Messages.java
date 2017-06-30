import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Created by Simo on 05/06/2017.
 */

public interface Messages extends Serializable {
    //TODO: migliorare la serializzazione
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
    public double lambda;
    public double[] similarity_row;
    public double[] similarity_col;
    public int selfID;

    public Initialize(double lambda, double[] similarity_row, double[] similarity_col, int selfID) {
        this.lambda = lambda;
        this.similarity_row = similarity_row;
        this.similarity_col = similarity_col;
        this.selfID = selfID;
    }
}

class Value implements Messages {
    public Double value;
    public int sender;

    public Value(Double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

class Self implements Messages {
    public ActorRef self;

    public Self(ActorRef self){
        this.self = self;
    }
}

class Die implements Messages{

}

class GetNeighbors implements Messages {

}

class AskExemplar implements Messages {

}

class Start implements Messages {

}

class Ready implements Messages {
    public int self;

    public Ready(){}
    public Ready(int self){this.self = self;}
}