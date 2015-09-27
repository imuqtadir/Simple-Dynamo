package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by amair on 4/18/15.
 */
public class Message  implements Serializable {

    private static final long serialVersionUID = 1L;

    public int type; // 1 ---> Insert into urself,
    // 2---> Insert into replica;
    // 3---> send ur results back for a particular selection;
    // 4-->Votes received for a particular selection;
    //7-->select specific record from your avd
    public String OriginPort;
    public String uri; // Uri used for insert, query, delete (converted to string)
    public String KeyValue;
    public String sendPort;
    public String selection;
    public String queryOrigin;
    public String queryResponse;
    public String queryNextNode;
    public String queryNextToNextNode;
    public int queryResponseVersion;
    public boolean retry=false;


    public Message(){

    }


}


