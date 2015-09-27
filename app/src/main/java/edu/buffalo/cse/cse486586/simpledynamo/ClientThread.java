package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by amair on 5/5/15.
 */

import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import android.util.Log;

public class ClientThread extends SimpleDynamoProvider implements Runnable {

    Message msgToSend;
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    public ClientThread(Message msg) {
        super();
        this.msgToSend = msg;
    }

    @Override
    public void run() {

        String portNumber = msgToSend.sendPort;																				//extract port number
        try{
            Log.e(TAG, "In ClientThread, the msg[0] details are: type" + msgToSend.type + "The key is: " + msgToSend.KeyValue + "Port is" + msgToSend.OriginPort + "Sending this to" + msgToSend.sendPort);

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(portNumber));	//create socket
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(msgToSend);																		//write msg to output stream
            objectOutputStream.flush();
            objectOutputStream.close();
            socket.close();
            Log.e(TAG, "SUCCESS in ClientThread" + msgToSend.selection+"Port was :"+msgToSend.sendPort);

        }
        catch (Exception e) {
            Log.e(TAG, "ClientIO Exception in Client Thread" + msgToSend.selection + " KEYLOCATION: " + msgToSend.sendPort + "Type: " + msgToSend.type + "Forward to:" + msgToSend.sendPort);

        }

    }


}