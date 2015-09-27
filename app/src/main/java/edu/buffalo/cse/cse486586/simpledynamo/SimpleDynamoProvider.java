package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    public static String myPort; //Sender's Port
    public static String portStr; // myAVD name

    static final String CREATE_TABLE = "CREATE TABLE message_tbl (" + "key" + " TEXT PRIMARY KEY, " + "value" + " TEXT NOT NULL, version TEXT)";

    static final String TABLE_NAME = "message_tbl";
    static final String ID = "key";
    private final ReentrantLock lock = new ReentrantLock();


    static final String MESSAGE = "value";

    static final int DATABASE_VERSION = 1;
    public static boolean isRecoveredFully = true;

   static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    static final String DATABASE_NAME = "DynamoDB";

    static final int SERVER_PORT = 10000;

    static String[] allAvds = {"5554", "5556", "5558", "5560", "5562"};

    public DBHelper db1;

    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledynamo.provider";

    static final String URL = "content://" + PROVIDER_NAME + "/SimpleDynamoActivity";

    static final Uri CONTENT_URI = Uri.parse(URL);
    Uri message_uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
    static final String[] allPorts = {"5554", "5556", "5558", "5560", "5562"};

    TreeMap<String, String> nodeMapping = new TreeMap<>();
    String[] mapKeys;

    public static Cursor globalCursor;
    public static Cursor globalCursorForParticularOutside;
    public static Cursor globalCursorforRecovery;

    public static boolean gotAllVotes;
    public static boolean gotAllVotesForRecover;

    public static int voteCount = 0;
    public static int voteCount1 = 0;
    public static int voteCountForParticularQuery = 0;
    public static int highestVersion = 0;

    public static String intermediateAllResults = "";
    public static String intermediateAllResultsforRecovery = "";
    public static long time = 0;
    public static int i = 0;
    public static String universalKey = "";
    public static boolean isRecovery = false;


    public static ConcurrentHashMap<String, String> KeyValPairsSeen = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> JustIn = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> KeyValPairsInserted = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Integer> keyToHighestVersionSoFar = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Integer> keyToNumberOfVotesSoFar = new ConcurrentHashMap<>();


    //ContentProvider Class starts here
    public static class DBHelper extends SQLiteOpenHelper {


        public DBHelper(Context context) {
            // TODO Auto-generated constructor stub
            super(context, DATABASE_NAME, null, DATABASE_VERSION);

        }

        @Override
        public void onCreate(SQLiteDatabase db) {

            // TODO Auto-generated method stub

            db.execSQL(CREATE_TABLE);
        }


        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.w(DBHelper.class.getName(),
                    "Upgrading database from version " + oldVersion + " to " + newVersion + ". Old data will be destroyed");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            onCreate(db);
        }
    }//ContentProvider Class ends here

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try {
            SQLiteDatabase db = db1.getWritableDatabase();
            if (KeyValPairsInserted.containsKey(selection)) {
                KeyValPairsInserted.remove(selection);
            }

            Log.e(TAG, "Inside Query where selection is" + selection);

            if (selection.equals("\"@\"")) {
                Log.e(TAG, "Delete Selection is matched to @");
                db.execSQL("delete from " + TABLE_NAME);

                return 0;
            }
            //We need to check for * now
            if (selection.equals("\"*\"")) {

                // db.rawQuery("SELECT * FROM message_tbl;", null);
                for (String i : allPorts) {

                    Message deleteAll = new Message();
                    deleteAll.selection = selection;
                    deleteAll.type = 8;
                    deleteAll.sendPort = Integer.toString(Integer.valueOf(i) * 2);
                    deleteAll.queryOrigin = myPort;
                    new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteAll);
                }


                Log.e(TAG, "Deleted all records across avds:");


                return 0;

            } else {
                Log.e(TAG, "Deleting specific record");

                //send to original
                String originOfRecord = getKeyLocation(selection);

                String nextNode = getNextNode(genHash(originOfRecord));
                String nextToNextNode = getNextToNextNode(genHash(originOfRecord));

                Log.e(TAG, "Delete: The key is" + selection + " and its location is" + originOfRecord + "Its replica1 and replica2 are" + nextNode + " " + nextToNextNode);
                Message deleteRecord = new Message();
                deleteRecord.selection = selection;
                deleteRecord.type = 7;
                deleteRecord.sendPort = Integer.toString(Integer.valueOf(originOfRecord) * 2);
                deleteRecord.queryOrigin = myPort;
                Log.e(TAG, "Sending delete command to the port:" + deleteRecord.sendPort + "for the selection" + selection);
                new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteRecord);

                //send to replica1
                Message deleteRecord1 = new Message();
                deleteRecord1.selection = selection;
                deleteRecord1.type = 7;
                deleteRecord1.sendPort = Integer.toString(Integer.valueOf(nextNode) * 2);
                deleteRecord1.queryOrigin = myPort;
                Log.e(TAG, "Sending delete command to the port:" + deleteRecord1.sendPort + "for the selection" + selection);
                new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteRecord1);


                //send to replica2
                Message deleteRecord2 = new Message();
                deleteRecord2.selection = selection;
                deleteRecord2.type = 7;
                deleteRecord2.sendPort = Integer.toString(Integer.valueOf(nextToNextNode) * 2);
                deleteRecord2.queryOrigin = myPort;
                Log.e(TAG, "Sending delete command to the port:" + deleteRecord2.sendPort + "for the selection" + selection);
                new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteRecord2);


            }


            return 0;
        } catch (NoSuchAlgorithmException e) {
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        try {
/*    while(isRecoveredFully){

    }*/
       /*     if (!isRecoveredFully) {
                SystemClock.sleep(1000);
                Log.e(TAG,"Slept in insert"+(String) values.get("key"));
            }*/
            long row;
            SQLiteDatabase db = db1.getWritableDatabase();
            String currentKey = (String) values.get("key");
            String currentValue = (String) values.get("value");

            //KeyValPairsInserted.put(currentKey, currentValue);
            JustIn.clear();
            JustIn.put(currentKey, currentValue);
            String currentKeyLocation = getKeyLocation(currentKey);
            String nextNode = getNextNode(genHash(currentKeyLocation));
            String nextToNextNode = getNextToNextNode(genHash(currentKeyLocation));
       /*     if (currentKeyLocation.equals(portStr) || nextNode.equals(portStr) || nextToNextNode.equals(portStr)) {
                Log.e(TAG, "the key: " + currentKey + " belongs to:" + currentKeyLocation + "And im either of the three:" + portStr);
                insertValue(currentKey, currentValue, message_uri.toString());
            }*/


            Log.e(TAG, "Insert: The location of the key:" + currentKey + "is here:" + currentKeyLocation);

            //forward to actual node

            Message insertToPass = new Message();
            insertToPass.KeyValue = currentKey + "," + currentValue;
            insertToPass.selection = currentKey + "," + currentValue;
            insertToPass.OriginPort = myPort;
            insertToPass.sendPort = Integer.toString(Integer.valueOf(currentKeyLocation) * 2);
            insertToPass.type = 1;
            insertToPass.uri = uri.toString();
            Log.e(TAG, "Insert Block:1 Forward Request to actual key location" + currentKey + " to the port" + insertToPass.sendPort);

           // new QueryMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertToPass);
            new Thread(new ClientThread(insertToPass)).start();




            //Send to Replica 1

            Message insertToReplica1 = new Message();
            insertToReplica1.KeyValue = currentKey + "," + currentValue;
            insertToReplica1.selection = currentKey + "," + currentValue;
            insertToReplica1.OriginPort = myPort;
            insertToReplica1.sendPort = Integer.toString(Integer.valueOf(nextNode) * 2);
            insertToReplica1.type = 2;
            insertToReplica1.uri = uri.toString();
            Log.e(TAG, "Insert Block:1 Forward Request to Replica 1, key is" + currentKey + " value is" + currentValue + "sending to the port" + insertToReplica1.sendPort);
            //new QueryMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertToReplica1);
            new Thread(new ClientThread(insertToReplica1)).start();

            //Send to Replica 2

            Message insertToReplica2 = new Message();
            insertToReplica2.KeyValue = currentKey + "," + currentValue;
            insertToReplica2.OriginPort = myPort;
            insertToReplica2.selection = currentKey + "," + currentValue;
            insertToReplica2.sendPort = Integer.toString(Integer.valueOf(nextToNextNode) * 2);

            insertToReplica2.type = 2;
            insertToReplica2.uri = uri.toString();
            Log.e(TAG, "Insert Block:1 Forward Request to Replica 2, key is" + currentKey + " value is" + currentValue + "sending to the port" + insertToReplica2.sendPort);

           // new QueryMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertToReplica2);
            new Thread(new ClientThread(insertToReplica2)).start();

        } catch (NoSuchAlgorithmException e) {

        }
        getContext().getContentResolver().notifyChange(uri, null);
        return uri;

        //if the key belong to one among the three


    }

    @Override
    public boolean onCreate() {

        try {
            Log.e(TAG,"Line one of Oncreate");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            // serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e(TAG,"Call for ServerTask ended");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            Log.e(TAG,"portstr and myport calculated");

        //Create a Tree map
            Log.e(TAG,"calling opening clietntakt");

        new OpeningClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null);
        //Server Task that listens always
            Log.e(TAG,"end of calling clienttask, done with oncreate");
            // SystemClock.sleep(1000);

            return true;


        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket Booh!" + Log.getStackTraceString(e));
            return false;
        }

        //return false;
    }

    private class OpeningClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msg) {
            Log.e(TAG,"Inside OpeningClient Task");
            boolean isMappingCreated = createNodeMapping();
            boolean isCreateArrayOfSortedKeysCreated = createArrayOfSortedKeys(nodeMapping);

            if (isTablePresentAlready()) {
                isRecovery = true;
                KeyValPairsInserted.clear();
                Log.e(TAG, "RECOVERY MODE ON!");
            }
            Log.e(TAG, "In: onCreate() Start the PARTY" + i++);

            db1 = new DBHelper(getContext());
   //         SQLiteDatabase db = db1.getWritableDatabase();
            if (isRecovery) {
                Thread one = new Thread() {

                    public void run() {


                        Log.e(TAG,"Inside recovery thread");
                        boolean isRecovered = isRecoveryMode();
                        Log.e(TAG, "isRecovered" + isRecovered);

                    }
                };

                one.start();

            }

            Log.e(TAG,"end of OpeningClient Task");
            return null;
        }


    } //ClientTask ends here

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        try {
            Log.e(TAG,"In query : "+selection);
       /*    if (!isRecoveredFully) {
                SystemClock.sleep(1000);
               Log.e(TAG,"Slept coz it was recovering");
            }*/
            SQLiteDatabase db = db1.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(TABLE_NAME);
            Cursor cursor;
            if (selection.equals("\"*\"")) {
                voteCount = 0;
                intermediateAllResults = "";
                for (String i : allPorts) {
                    Message forwardQueryToGetVote1 = new Message();
                    forwardQueryToGetVote1.OriginPort = myPort;
                    forwardQueryToGetVote1.queryOrigin = myPort;
                    forwardQueryToGetVote1.type = 5;
                    forwardQueryToGetVote1.selection = selection;
                    forwardQueryToGetVote1.sendPort = Integer.toString(Integer.valueOf(i) * 2);
                    Log.e(TAG, "In Query: sending to all ports and the key is" + selection);
                   // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote1);
                    new Thread(new ClientThread(forwardQueryToGetVote1)).start();

                }
                // gotAllVotes = true;

/*                while (gotAllVotes) {


                }*/

                SystemClock.sleep(2500);
                Log.e(TAG, "In Query:After sleep the vote count is:"+ voteCount);

                return globalCursor;


            } else if (selection.equals("\"@\"")) {
                if(!isRecoveredFully){
                    Log.e(TAG,"Sleep inside @");
                    SystemClock.sleep(2500);
                    Log.e(TAG,"Slept inside @"+isRecoveredFully);
                }
                cursor = db.rawQuery("SELECT key,value FROM message_tbl", null);
                Log.e(TAG, "Count is: @ " + cursor.getCount());
                return cursor;



               /* voteCount=0;
                intermediateAllResults="";
                String currentKeyLocation = portStr;
                String nextNode = getNextNode(genHash(currentKeyLocation));
                String previousNode = getPreviousNode(genHash(currentKeyLocation));

                Message forwardQueryToGetVote1 = new Message();
                forwardQueryToGetVote1.OriginPort = portStr;
                forwardQueryToGetVote1.queryOrigin = myPort;
                forwardQueryToGetVote1.type = 11;
                forwardQueryToGetVote1.selection = selection;
                forwardQueryToGetVote1.sendPort = Integer.toString(Integer.valueOf(currentKeyLocation) * 2);

                Log.e(TAG, " @ In Query: sending to myself and the key is" + selection + " KEYLOCATION: " + currentKeyLocation);
              // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote1);
                new Thread(new ClientThread(forwardQueryToGetVote1)).start();


                //send to next
                Message forwardQueryToGetVote2 = new Message();
                forwardQueryToGetVote2.OriginPort = portStr;
                forwardQueryToGetVote2.queryOrigin = myPort;
                forwardQueryToGetVote2.type = 11;
                forwardQueryToGetVote2.selection = selection;
                forwardQueryToGetVote2.sendPort = Integer.toString(Integer.valueOf(nextNode) * 2);
                Log.e(TAG, "@ In Query: sending to next port" + nextNode + " and the key is" + selection);
               // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote2);
                new Thread(new ClientThread(forwardQueryToGetVote2)).start();

                //send to previous
                Message forwardQueryToGetVote3 = new Message();
                forwardQueryToGetVote3.OriginPort = portStr;
                forwardQueryToGetVote3.queryOrigin = myPort;
                forwardQueryToGetVote3.type = 11;
                forwardQueryToGetVote3.selection = selection;
                forwardQueryToGetVote3.sendPort = Integer.toString(Integer.valueOf(previousNode) * 2);
                Log.e(TAG, "@ In Query: sending to previous port" + previousNode + " and the key is" + selection);
                //new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote3);
                new Thread(new ClientThread(forwardQueryToGetVote3)).start();


                Log.e(TAG, "Got @ and returning results from myself");

                SystemClock.sleep(1500);
                Log.e(TAG, "Vote Count final after 2500 wait @" + voteCount);
                if(voteCount==0) {
                    if(!isRecoveredFully){
                      SystemClock.sleep(800);
                    }
                    cursor = db.rawQuery("SELECT key,value FROM message_tbl", null);
                    Log.e(TAG, "Count is: @ " + cursor.getCount());
                    return cursor;
                }
                Log.e(TAG, "Vote Count final for @" + voteCount);
                while(!(voteCount>=2)){

                }

                //Log.e(TAG, "The query result for the selection" + selection + "returns following" + ConvertCursorToString(cursor));
                return globalCursor;*/

            }

            Log.e(TAG, "Got a ? Query to me: " + portStr+"The key is:"+selection);

            String currentKeyLocation = getKeyLocation(selection);
            String nextNode = getNextNode(genHash(currentKeyLocation));
            String nextToNextNode = getNextToNextNode(genHash(currentKeyLocation));
            Log.e(TAG, "In Query: The key:" + selection + " is inside the AVD:" + currentKeyLocation);

       /*     if (currentKeyLocation.equals(portStr) || nextNode.equals(portStr) || nextToNextNode.equals(portStr) && isRecoveredFully) {


                String newSelection = "key='" + selection + "'";
                //search key logic
                Cursor cursorForParticular = queryBuilder.query(db, new String[]{"key", "value"}, newSelection,
                        null, null, null, null);

                Log.e(TAG, "the key" + selection + " belongs to:" + currentKeyLocation + "And im either of the three:" + portStr + " the keyval within me is:" + ConvertCursorToStringForKeyVal(cursorForParticular));

                return cursorForParticular;
            } else {*/
              /*  while(!isRecoveredFully){

                }*/
            //   Cursor tempCur = null;
            Log.e(TAG, "Forwarding series of messages to get results :" + selection);
           /*     if (KeyValPairsInserted.containsKey(selection)) {
                    String tempVal = KeyValPairsInserted.get(selection);
                    tempCur = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(selection + "," + tempVal));
                    // return tempCur;
                }*/
            //send to original port

            //SystemClock.sleep(1000);
            KeyValPairsSeen.remove(selection);
            keyToHighestVersionSoFar.remove(selection);
            keyToNumberOfVotesSoFar.remove(selection);

            Message forwardQueryToGetVote1 = new Message();
            forwardQueryToGetVote1.OriginPort = myPort;
            forwardQueryToGetVote1.queryOrigin = myPort;
            forwardQueryToGetVote1.type = 3;
            forwardQueryToGetVote1.selection = selection;
            forwardQueryToGetVote1.sendPort = Integer.toString(Integer.valueOf(currentKeyLocation) * 2);
            forwardQueryToGetVote1.queryNextNode = Integer.toString(Integer.valueOf(nextNode) * 2);
            forwardQueryToGetVote1.queryNextToNextNode = Integer.toString(Integer.valueOf(nextToNextNode) * 2);
            Log.e(TAG, "In Query: sending to original port and the key is" + selection + " KEYLOCATION: " + currentKeyLocation);
           // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote1);
            new Thread(new ClientThread(forwardQueryToGetVote1)).start();

            //send to replica 1
            Message forwardQueryToGetVote2 = new Message();
            forwardQueryToGetVote2.OriginPort = myPort;
            forwardQueryToGetVote2.queryOrigin = myPort;
            forwardQueryToGetVote2.type = 3;
            forwardQueryToGetVote2.selection = selection;
            forwardQueryToGetVote2.sendPort = Integer.toString(Integer.valueOf(nextNode) * 2);

            forwardQueryToGetVote2.queryNextNode = Integer.toString(Integer.valueOf(nextToNextNode) * 2);
            forwardQueryToGetVote2.queryNextToNextNode = Integer.toString(Integer.valueOf(nextToNextNode) * 2);
            Log.e(TAG, "In Query: sending to replica1 port" + nextNode + " and the key is" + selection);
           // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote2);
            new Thread(new ClientThread(forwardQueryToGetVote2)).start();
/*
                //send to replica 2
                Message forwardQueryToGetVote3 = new Message();
                forwardQueryToGetVote3.OriginPort = myPort;
                forwardQueryToGetVote3.queryOrigin = myPort;
                forwardQueryToGetVote3.type = 3;
                forwardQueryToGetVote3.selection = selection;
                forwardQueryToGetVote3.sendPort = Integer.toString(Integer.valueOf(nextToNextNode) * 2);
                Log.e(TAG, "In Query: sending to replica2 port" + nextToNextNode + " and the key is" + selection);
                new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToGetVote3);*/

            synchronized (this) {

                // lock.lock();

                highestVersion = 0;

                universalKey = selection;
                Log.e(TAG, "Sleeping for the key result" + selection + "time now is:" + System.currentTimeMillis());

                //SystemClock.sleep(3500);
                voteCountForParticularQuery = 0;
                time = 0;
                gotAllVotes = false;
                //voteCount = 0;
                //highestVersion = 0;
                // Log.e(TAG, "Timer over:" + System.currentTimeMillis() + " for the key" + selection + " keyval is " + ConvertCursorToString(globalCursor));
                SystemClock.sleep(2000);
             /*       if((!KeyValPairsSeen.containsKey(universalKey)) && KeyValPairsInserted.containsKey(selection) && tempCur!=null){
                        Log.e(TAG,"Too early, im giving my temp results"+universalKey+"KeyVal is"+ConvertCursorToString(tempCur));
                        KeyValPairsInserted.remove(selection);
                        return tempCur;
                    }*/
                if (JustIn.containsKey(universalKey)) {
                    String tempKV = universalKey + "," + JustIn.get(universalKey);

                    globalCursorForParticularOutside = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(tempKV));
                    Log.e(TAG, "Got results for key in JUSTIN :" + universalKey + "keyval is: " + ConvertCursorToStringForKeyVal(globalCursorForParticularOutside));

                    return globalCursorForParticularOutside;
                } else {
                    Log.e(TAG, "Situation after wait for the key" + universalKey + "!KeyValPairsSeen.containsKey(universalKey) " + !KeyValPairsSeen.containsKey(universalKey) + "Gotallvotes" + gotAllVotes);
                 /*      if(!keyToNumberOfVotesSoFar.containsKey(universalKey)){
                           Log.e(TAG,"Still does not have any replies");
                           SystemClock.sleep(2000);
                       }*/
                    while(!keyToNumberOfVotesSoFar.containsKey(universalKey)){

                    }
                    Log.e(TAG,"We have votes now"+keyToNumberOfVotesSoFar.get(universalKey));
                    while (!KeyValPairsSeen.containsKey(universalKey) && !(keyToNumberOfVotesSoFar.get(universalKey) == 1)) {

                    }
                    Log.e(TAG, "Just on time, Im giving the calculated results" + universalKey);

                    String tempKV = universalKey + "," + KeyValPairsSeen.get(universalKey);

                    globalCursorForParticularOutside = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(tempKV));
               /*     gotAllVotes = true;
                    while (gotAllVotes) {


                    }*/
                    //SystemClock.sleep(500);
                    Log.e(TAG, "Got results for key:" + universalKey + "keyval is: " + ConvertCursorToStringForKeyVal(globalCursorForParticularOutside));
                    return globalCursorForParticularOutside;

                }
            }
            //}

        } catch (NoSuchAlgorithmException e) {

        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            try {
                Log.e(TAG,"Inside server task");
                ServerSocket serverSocket = sockets[0];

                //  serverSocket.setReuseAddress(true);

            /*
             *  Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
                while (true) {


                    Socket soc = serverSocket.accept();
                    Log.e(TAG, "Server socket accepted");
                    ObjectInputStream ois = new ObjectInputStream(soc.getInputStream());
                    Log.e(TAG, "ObjectInputStream");


                    Object received = ois.readObject();
                    //ois.close();

                    Log.e(TAG, "readObject");

                    Message receivedMessage = (Message) received;
                    Log.e(TAG, "In Server, Received Message in SeverTask with type: " + receivedMessage.type + " Origin is " + receivedMessage.OriginPort + " Key is " + receivedMessage.selection + "For inserting: KEYVAL" + receivedMessage.KeyValue);

                    //The Key belongs to you, just put it into yourself
                    if (receivedMessage.type == 1) {
                        //insert into yourself and forward to next two node
                        Log.e(TAG, "TYPE: 1 Received! " + receivedMessage.KeyValue);

                        String currentKey = receivedMessage.KeyValue.split(",")[0].trim();
                        Log.e(TAG, "TYPE: 1 currentKey: " + currentKey);

                        String currentValue = receivedMessage.KeyValue.split(",")[1].trim();
                        Log.e(TAG, "TYPE: 1 currentValue: " + currentValue);

                        //Insert into yourself
                        boolean isInserted = insertValue(currentKey, currentValue, receivedMessage.uri);
                        Log.e(TAG, "TYPE: 1 Successfully inserted key" + currentKey + "inserted: " + isInserted);

                    }

                    //You are the replica, just save it
                    if (receivedMessage.type == 2) {
                        Log.e(TAG, "TYPE: 2 Received! " + receivedMessage.KeyValue);
                        String currentKey = receivedMessage.KeyValue.split(",")[0].trim();
                        Log.e(TAG, "TYPE: 2 currentKey: " + currentKey);

                        String currentValue = receivedMessage.KeyValue.split(",")[1].trim();
                        Log.e(TAG, "TYPE: 2 currentValue: " + currentValue);

                        boolean isInserted = insertValue(currentKey, currentValue, receivedMessage.uri);
                        Log.e(TAG, "TYPE: 2 Successfully inserted key" + currentKey + "inserted: " + isInserted);
                    }

                    //You are asked to VOTE
                    if (receivedMessage.type == 3) {

                        Log.e(TAG, "TYPE: 3 Received! ");
                        String keySelection = receivedMessage.selection;
                        Log.e(TAG, "TYPE: 3 keySelection: " + keySelection);

                        String results = fetchQueryResults(keySelection);
                  /*      if (results.isEmpty()) {
                            Log.e(TAG, "Results were empty, wait started!" + keySelection);
                            SystemClock.sleep(2000);
                            Log.e(TAG, "Results were empty, wait done!" + keySelection);
                            results = fetchQueryResults(keySelection);
                        }*/

                        Log.e(TAG, "TYPE: 3 results: " + results);

                        Message myVote = new Message();
                        myVote.selection = keySelection;
                        myVote.OriginPort = portStr;
                        myVote.sendPort = receivedMessage.queryOrigin;
                        myVote.type = 4;
                        myVote.queryResponse = results;
                        //key,value,version
                        myVote.queryResponseVersion = getVersion(keySelection);
                        //myVote.queryResponseVersion = 0;
                        Log.e(TAG, "TYPE: 3 Sending myVote: " + myVote.queryResponse + " from AVD:" + myPort);

                       // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myVote);
                        new Thread(new ClientThread(myVote)).start();
                    }

                    //You are the coordinator and you got a vote
                    if (receivedMessage.type == 4) {
                        Log.e(TAG, "Inside Type 4" + receivedMessage.queryResponse + "For the key:" + receivedMessage.selection + "Response version: " + receivedMessage.queryResponseVersion);
                       /* if(isRecoveredFully && !keyToNumberOfVotesSoFar.containsKey(receivedMessage.selection)) {
                            Log.e(TAG,"Cleared JustIn"+receivedMessage.selection);
                            JustIn.clear();
                        }*/
                        if (receivedMessage.queryResponse.isEmpty() || receivedMessage.queryResponse==null) {
                            Log.e(TAG, "Inside Type 4 isEmpty" + receivedMessage.queryResponse + "For the key:" + receivedMessage.selection);
                            if (keyToNumberOfVotesSoFar.containsKey(receivedMessage.selection)) {
                                int tempVoteNumber = keyToNumberOfVotesSoFar.get(receivedMessage.selection) + 1;
                                Log.e(TAG, "In isEMpty of Type: 4 keyToNumberOfVotesSofar (+1) is" + tempVoteNumber);
                                keyToNumberOfVotesSoFar.put(receivedMessage.selection, tempVoteNumber);
                                if(keyToHighestVersionSoFar.containsKey(receivedMessage.selection)){

                                }
                                else{
                                    keyToHighestVersionSoFar.put(receivedMessage.selection, -1);
                                }


                            } else {
                                keyToNumberOfVotesSoFar.put(receivedMessage.selection, 0);
                                if(keyToHighestVersionSoFar.containsKey(receivedMessage.selection)){

                                }
                                else{
                                    keyToHighestVersionSoFar.put(receivedMessage.selection, -1);
                                }
                            }

                        }


                        // voteCountForParticularQuery++;
                        else {
                            Log.e(TAG, "Result is not empty");
                            Log.e(TAG, "!keyToNumberOfVotesSoFar.containsKey(receivedMessage.queryResponse.split(\",\")[0])" + (!keyToNumberOfVotesSoFar.containsKey(receivedMessage.queryResponse.split(",")[0])));
                            Log.e(TAG, "Result not empty Total KeyVal is:" + receivedMessage.queryResponse);
                            Log.e(TAG, "keyToHighestVersionSoFar.get(receivedMessage.selection) " + keyToHighestVersionSoFar.containsKey(receivedMessage.selection));
                            if (!keyToNumberOfVotesSoFar.containsKey(receivedMessage.queryResponse.split(",")[0]) || !keyToHighestVersionSoFar.containsKey(receivedMessage.queryResponse.split(",")[0])) {

                                gotAllVotes = false;
                                Log.e(TAG, "TYPE: 4 voteCountForParticularQuery =0  Response is:" + receivedMessage.queryResponse + "version is:" + receivedMessage.queryResponseVersion);

                                keyToNumberOfVotesSoFar.put(receivedMessage.queryResponse.split(",")[0], 0);

                                KeyValPairsSeen.put(receivedMessage.queryResponse.split(",")[0], receivedMessage.queryResponse.split(",")[1]);
                                keyToHighestVersionSoFar.put(receivedMessage.queryResponse.split(",")[0], receivedMessage.queryResponseVersion);

                            }
                            // Log.e(TAG,"Type 4: it should be containing the key by now: for the key: "+receivedMessage.queryResponse.split(",")[0]+"The version is"+keyToHighestVersionSoFar.get(receivedMessage.queryResponse.split(",")[0]));
                            else if (keyToHighestVersionSoFar.get(receivedMessage.selection) < receivedMessage.queryResponseVersion) {
                                Log.e(TAG, "TYPE: 4 Got a higher Version  " + receivedMessage.queryResponseVersion + "From the AVD: " + receivedMessage.OriginPort + "And its response is:" + receivedMessage.queryResponse);

                                keyToHighestVersionSoFar.remove(receivedMessage.queryResponse.split(",")[0]);
                                keyToHighestVersionSoFar.put(receivedMessage.queryResponse.split(",")[0], receivedMessage.queryResponseVersion);

                                KeyValPairsSeen.remove(receivedMessage.queryResponse.split(",")[0]);
                                KeyValPairsSeen.put(receivedMessage.queryResponse.split(",")[0], receivedMessage.queryResponse.split(",")[1]);

                                int tempVoteNumber = keyToNumberOfVotesSoFar.get(receivedMessage.queryResponse.split(",")[0]) + 1;
                                Log.e(TAG, "In ElseIf of Type: 4 keyToNumberOfVotesSofar (+1) is" + tempVoteNumber+"key "+receivedMessage.selection);
                                keyToNumberOfVotesSoFar.put(receivedMessage.queryResponse.split(",")[0], tempVoteNumber);


                            } else {
                                Log.e(TAG, "Some type came but neither was it 0 or its version was greater" + receivedMessage.queryResponse + "Version:" + receivedMessage.queryResponseVersion);
                                int tempVoteNumber = keyToNumberOfVotesSoFar.get(receivedMessage.queryResponse.split(",")[0]) + 1;
                                Log.e(TAG, "In Else of Type: 4 keyToNumberOfVotesSofar (+1) is" + tempVoteNumber+"For the key"+receivedMessage.selection);
                                keyToNumberOfVotesSoFar.put(receivedMessage.queryResponse.split(",")[0], tempVoteNumber);

                            }
                        }

                        //|| ((System.currentTimeMillis()-time)>=2500)
                        if (keyToNumberOfVotesSoFar.get(receivedMessage.selection) == 1) {
                            //Log.e(TAG, "TYPE: 4 Time difference is " + (System.currentTimeMillis() - time));
                            Log.e(TAG, "TYPE: 4 Vote count is 1 and complete"+receivedMessage.selection);
                            //time = 0;
                            //keyToNumberOfVotesSoFar.remove(receivedMessage.queryResponse.split(",")[0]);
                            //  keyToHighestVersionSoFar.remove(receivedMessage.queryResponse.split(",")[0]);
                            //highestVersion = 0;

                            //  Log.e(TAG, "Got all VOTES for key:" + receivedMessage.selection + " and the result is" + KeyValPairsSeen.get(receivedMessage.queryResponse.split(",")[0] );
                            gotAllVotes = true;
                        }

                    }

                    //You are asked to VOTE for *
                    if (receivedMessage.type == 5) {
                        Log.e(TAG, "TYPE: 5 Received! ");
                        String keySelection = receivedMessage.selection;
                        Log.e(TAG, "TYPE: 5 keySelection: " + keySelection);

                        String results = fetchAllRecords(keySelection);
                        Log.e(TAG, "TYPE: 5 key selection is" + keySelection + "results from:" + portStr + " are total: " + results);
                        // Log.e(TAG, "TYPE: 5 results: " + results);

                        Message myAllRecords = new Message();
                        myAllRecords.selection = keySelection;
                        myAllRecords.OriginPort = myPort;
                        myAllRecords.sendPort = receivedMessage.queryOrigin;
                        myAllRecords.type = 6;
                        myAllRecords.queryResponse = results;

                        // Log.e(TAG, "TYPE: 5 Sending myVote: " + myAllRecords.queryResponse);
                       // new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myAllRecords);
                        new Thread(new ClientThread(myAllRecords)).start();
                    }

                    //coordinator for *
                    if (receivedMessage.type == 6) {
                        voteCount++;

                        Log.e(TAG, "TYPE: 6 Received, We got results from " + receivedMessage.OriginPort + "Vote number is: " + voteCount);


                        if (voteCount <= 4) {
                            Log.e(TAG, "TYPE: 6 voteCount= " + voteCount);
                            intermediateAllResults = intermediateAllResults + receivedMessage.queryResponse;
                            if (voteCount == 4) {
                                globalCursor = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(intermediateAllResults));
                            }

                        }

                        if (voteCount == 5) {
                            time = 0;
                            intermediateAllResults = intermediateAllResults + receivedMessage.queryResponse;
                            globalCursor = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(intermediateAllResults));
                            //voteCount = 0;
                            intermediateAllResults = "";
                            Log.e(TAG, "Got all VOTES for * : and the final result is");
                            // gotAllVotes = false;

                        }

                    }//ends

                    if (receivedMessage.type == 7) {
                        Log.e(TAG, "TYPE: 7 particular record is= " + receivedMessage.selection);
                        deleteParticularRecord(receivedMessage.selection);
                    }

                    if (receivedMessage.type == 8) {
                        Log.e(TAG, "TYPE: 8 all record deletion= " + receivedMessage.selection);
                        deleteAll();
                    }

                    if (receivedMessage.type == 9) {

                        new RecoveryMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
/*                        Log.e(TAG, "TYPE: 9 Received! I need to send my results to this AVD so that it recovers: " + receivedMessage.OriginPort);
                        String keySelection = receivedMessage.selection;
                        Log.e(TAG, "TYPE: 9 keySelection: " + keySelection);

                        String tempResults = fetchAllRecords(keySelection);
                        String results = filterKeyValues(tempResults, receivedMessage.OriginPort);
                        Log.e(TAG, "TYPE: 9 key selection is" + keySelection + "results from:" + portStr + " filtered results total: " + results);
                        // Log.e(TAG, "TYPE: 5 results: " + results);

                        Message myAllRecords = new Message();
                        myAllRecords.selection = keySelection;
                        myAllRecords.OriginPort = myPort;
                        myAllRecords.sendPort = receivedMessage.queryOrigin;
                        myAllRecords.type = 10;
                        myAllRecords.queryResponse = results;

                        // Log.e(TAG, "TYPE: 5 Sending myVote: " + myAllRecords.queryResponse);
                        //new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myAllRecords);
                        new Thread(new ClientThread(myAllRecords)).start();*/

                    }

                    //got results for recovery

                    if (receivedMessage.type == 10) {
                        Log.e(TAG, "Type: 10 Received messages to recover!" + receivedMessage.OriginPort + "Response:" + receivedMessage.queryResponse + "Vote number out of 3:" + voteCount1);
                        // String filterRecords = filterKeyValues(receivedMessage.queryResponse, receivedMessage.OriginPort);

                       // boolean isInserted = insertFilteredValues(receivedMessage.queryResponse);
                        new InsertMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
                      //  new Thread(new InsertThread(receivedMessage.queryResponse)).start();

                        voteCount1++;
                        if (voteCount1 >= 3) {
                            isRecoveredFully = true;
                            //voteCount1 = 0;
                        }
                    }

                    if (receivedMessage.type == 11) {
                        new QueryMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMessage);
/*                        if (receivedMessage.selection.equals("\"@\"")) {

                            Log.e(TAG, "TYPE: 11 Received for @");
                            if(!isRecoveredFully){
                                Log.e(TAG, "TYPE: 11 sleeping");
                                SystemClock.sleep(1500);
                                Log.e(TAG, "TYPE: 11 sleeping done, is recovered fully"+isRecoveredFully);
                            }

                            String keySelection = receivedMessage.selection;
                            Log.e(TAG, "TYPE: 11 keySelection: " + keySelection);


                            String tempResults = fetchAllRecords(keySelection);
                            String results = filterKeyValues(tempResults, receivedMessage.OriginPort);
                            Log.e(TAG, "TYPE: 11 key selection is" + keySelection + "results from:" + portStr + " are total: " + results);
                            // Log.e(TAG, "TYPE: 5 results: " + results);

                            Message myAllRecords = new Message();
                            myAllRecords.selection = keySelection;
                            myAllRecords.OriginPort = myPort;
                            myAllRecords.sendPort = receivedMessage.queryOrigin;
                            myAllRecords.type = 12;
                            myAllRecords.queryResponse = results;
                            //new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myAllRecords);
                            new Thread(new ClientThread(myAllRecords)).start();
                        }*/
                    }


                    if (receivedMessage.type == 12) {
                        Log.e(TAG, "TYPE: 12 Received for @");
                        voteCount++;
                      //  new Thread(new RecoveryInsertThread(receivedMessage.queryResponse)).start();

                        if (receivedMessage.selection.equals("\"@\"")) {
                            Log.e(TAG, "TYPE: 12 Received for @, We got results from " + receivedMessage.OriginPort);
                            Log.e(TAG, "TYPE: 12 Vote number:" + voteCount + " coming from: " + receivedMessage.OriginPort + "As of now results are:" + intermediateAllResults);
                            if (voteCount < 2) {
                                Log.e(TAG, "TYPE: 12 less than 2 voteCount= " + voteCount);
                                intermediateAllResults = intermediateAllResults + receivedMessage.queryResponse;
                                // globalCursor = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(intermediateAllResults));
                                // Log.e(TAG, "Added results for : " + receivedMessage.OriginPort + "Total is now: " + intermediateAllResults);

                            }

                            if (voteCount >= 2) {

                                intermediateAllResults = intermediateAllResults + receivedMessage.queryResponse;
                                globalCursor = convertMapToCursorForKeyVal(convertStringToMapForKeyVal(intermediateAllResults));

                                Log.e(TAG, "Got all VOTES for @ : and the final result is" + ConvertCursorToStringForKeyVal(globalCursor));
                                // gotAllVotes = false;

                            }
                        }
                    }
                }
            } catch (IOException ex) {

                Log.e(TAG,"Server booh! IOEXCEPTION" +ex);
                Log.e(TAG, Log.getStackTraceString(ex));
                ex.printStackTrace();
            } catch (ClassNotFoundException ex) {
                ex.printStackTrace();
            }


            return null;
        }
    }


    //ClientTask starts here
    private class MessageClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msg) {
            try {
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                Log.e(TAG, "In MessageClientTask, the msg[0] details are: type" + msg[0].type + "The key is: " + msg[0].selection + "Port is" + msg[0].OriginPort + "Sending this to" + msg[0].sendPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msg[0].sendPort));
                // socket.setSoTimeout(100);
                //Log.e(TAG, "Now requesting to join the ring from " + msg[0].myPort + "this is send to " + msg[0].sendPort);
                OutputStream os = socket.getOutputStream();
                ObjectOutputStream msgObject = new ObjectOutputStream(os);
                msgObject.writeObject(msg[0]);

                Log.e(TAG, "SUCCESS in MessageClientTask" + msg[0].selection);

                socket.close();
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "MessageClientTask Timed out for the key" + msg[0].selection + " Exception:" + e);
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                Log.e(TAG, e.getMessage());
                if (msg[0].type == 3) {
                    Message forwardQueryToReplica1 = new Message();
                    forwardQueryToReplica1.OriginPort = msg[0].OriginPort;
                    forwardQueryToReplica1.queryOrigin = msg[0].queryOrigin;
                    forwardQueryToReplica1.type = msg[0].type;
                    forwardQueryToReplica1.selection = msg[0].selection;
                    forwardQueryToReplica1.sendPort = msg[0].queryNextNode;
                    forwardQueryToReplica1.queryNextNode = msg[0].queryNextToNextNode;

                    Log.e(TAG, "ClientIO Exception" + msg[0].selection + " KEYLOCATION: " + msg[0].sendPort + "Type: " + msg[0].type + "Forward to:" + forwardQueryToReplica1.sendPort);
                    new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardQueryToReplica1);
                }
            }

            return null;
        }


    } //ClientTask ends here

    //ClientTask starts here
    private class RecoveryMessageClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msg) {
            Message receivedMessage = msg[0];
            Log.e(TAG, "TYPE: 9 Received! I need to send my results to this AVD so that it recovers: " + receivedMessage.OriginPort);
            String keySelection = receivedMessage.selection;
            Log.e(TAG, "TYPE: 9 keySelection: " + keySelection);

            String tempResults = fetchAllRecords(keySelection);
            String results = filterKeyValues(tempResults, receivedMessage.OriginPort);
            Log.e(TAG, "TYPE: 9 key selection is" + keySelection + "results from:" + portStr + " filtered results total: " + results);
            // Log.e(TAG, "TYPE: 5 results: " + results);

            Message myAllRecords = new Message();
            myAllRecords.selection = keySelection;
            myAllRecords.OriginPort = myPort;
            myAllRecords.sendPort = receivedMessage.queryOrigin;
            myAllRecords.type = 10;
            myAllRecords.queryResponse = results;

            // Log.e(TAG, "TYPE: 5 Sending myVote: " + myAllRecords.queryResponse);
            //new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myAllRecords);
            new Thread(new ClientThread(myAllRecords)).start();
            return null;
        }


    } //ClientTask ends here

    //ClientTask starts here
    private class QueryMessageClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msg) {

            Message receivedMessage = msg[0];
            if (receivedMessage.selection.equals("\"@\"")) {

                Log.e(TAG, "TYPE: 11 Received for @");


                String keySelection = receivedMessage.selection;
                Log.e(TAG, "TYPE: 11 keySelection: " + keySelection);


                String tempResults = fetchAllRecords(keySelection);
                String results = filterKeyValues(tempResults, receivedMessage.OriginPort);
                Log.e(TAG, "TYPE: 11 key selection is" + keySelection + "results from:" + portStr + " are total: " + results);
                // Log.e(TAG, "TYPE: 5 results: " + results);

                Message myAllRecords = new Message();
                myAllRecords.selection = keySelection;
                myAllRecords.OriginPort = myPort;
                myAllRecords.sendPort = receivedMessage.queryOrigin;
                myAllRecords.type = 12;
                myAllRecords.queryResponse = results;
                //new MessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myAllRecords);
                new Thread(new ClientThread(myAllRecords)).start();
            }
            return null;
        }


    } //ClientTask ends here

    //ClientTask starts here
    private class InsertMessageClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msg) {

            Message receivedMessage = msg[0];
            boolean isInserted = insertFilteredValues(receivedMessage.queryResponse);
            return null;
        }


    } //ClientTask ends here


    private boolean insertValue(String key, String value, String uri) {
        db1 = new DBHelper(getContext());
        SQLiteDatabase db = db1.getWritableDatabase();
        Log.e(TAG, "Inserting key:" + key + " value:" + value + " and the port is:" + myPort);

        if (isPresentAlready(key)) {
            Log.e(TAG, "Just the key is present :" + key + "And its new value is: " + value);
            String key1 = "key= '" + key + "'";
            ContentValues contentValues = new ContentValues();
            contentValues.put("key", key.trim());
            contentValues.put("value", value.trim());
            String versionValue = Integer.toString(getVersion(key));

            contentValues.put("version", Integer.toString(Integer.valueOf(versionValue) + 1));
            db.insertWithOnConflict(TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
            //Log.e(TAG, "In update: rows effected are:" + row);
            Log.e(TAG, "Row was updated, for the key" + key + "And updated value is:" + value + "The latest version is:" + Integer.toString(Integer.valueOf(versionValue) + 1));

        } else {
            Log.e(TAG, "Inserted new row, key:" + key + " value:" + value);
            ContentValues contentValues = new ContentValues();
            contentValues.put("key", key.trim());
            contentValues.put("value", value.trim());

            //String versionValue = Integer.toString(getVersion(key));
            contentValues.put("version", "0");
            //  Uri ReceivedMessage_uri = Uri.parse(uri);
            // String version = "0";


            db.insertWithOnConflict(TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
            // db.execSQL("INSERT OR REPLACE INTO message_tbl (key, value,version) VALUES (?, ?,?)", args);


            Log.e(TAG, "Inserting key:" + key + " into the device: " + portStr);
            // long i = db.insert(TABLE_NAME,null,contentValues);
            // Log.e(TAG,"Insert status is"+i);
        }


        return true;
    }

    private boolean isKeyValuePresent(String key, String value) {

        String resultKeyValue = fetchQueryResults(key);

        if (resultKeyValue.contains(",")) {
            String[] keyValue = resultKeyValue.split(",");
/*                for (String i : keyValue) {
                    Log.e(TAG, "keyValue split into" + i);
                }*/
            if (value.equals(keyValue[1].trim())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }


    }

    private boolean newInsertFilteredValues(String KeyValue) {
        Log.e(TAG, "insertFilteredValues : " + KeyValue);
        db1 = new DBHelper(getContext());
        int i = 0;
        SQLiteDatabase db = db1.getWritableDatabase();
        String[] lines = KeyValue.split("\\r?\\n");

        for (String lin : lines) {

            if (lin.contains(",")) {
                String[] keyValue = lin.split(",");

                String key = keyValue[0].trim();
                String value = keyValue[1].trim();

                //  if (isPresentAlready(key)) {

                //}
                // else{
                ContentValues contentValues = new ContentValues();
                contentValues.put("key", key);
                contentValues.put("value", value);
                // contentValues.put("version", "0");
                Cursor cursor = getContext().getContentResolver().query(message_uri, null, key, null, null);
                String LatestKeyValue = ConvertCursorToString(cursor);
                Log.e(TAG, "Latest keyValue pair is: " + LatestKeyValue);
                String[] newKV = LatestKeyValue.split(",");
                String newKey = newKV[0].trim();
                String newValue = newKV[1].trim();

                Log.e(TAG, "newInsertFilteredValues Key:" + key + "New key" + newKey);
                Log.e(TAG, "newInsertFilteredValues Value:" + value + "New Value" + newValue);
                insertValue(newKey, newValue, "");


                //  String version = "0";
                //  String[] args = {key, value, version};
                //db.execSQL("INSERT OR REPLACE INTO message_tbl (key, value,version) VALUES (?, ?,?)", args);
                //Log.e(TAG, "Inserting key:" + key + " into the device: " + portStr + " the version is" + version);
                i++;
                // }
            }


        }
        Log.e(TAG, "Total Insertions for filtered is :" + i);
        return true;
    }

    public boolean insertFilteredValues(String KeyValue) {
        // Log.e(TAG,"insertFilteredValues : "+KeyValue);
        DBHelper d2 = new SimpleDynamoProvider.DBHelper(getContext());
        int i = 0;

        SQLiteDatabase db = d2.getWritableDatabase();
        String[] lines = KeyValue.split("\\r?\\n");

        for (String lin : lines) {

            //  Log.e(TAG, "inside insertFiltereValues lin is:" + lin);

            if (lin.contains(",")) {
                String[] keyValue = lin.split(",");

                String key = keyValue[0].trim();


                String value = keyValue[1].trim();
                String version = keyValue[2].trim();

                ContentValues contentValues = new ContentValues();
                contentValues.put("key", key);
                contentValues.put("value", value);
                contentValues.put("version", version);

                if (isPresentAlready(key)) {
                    Log.e(TAG, "Just the key is present :" + key + "And its new value is: " + value);


                    int existingVersion = getVersion(key);

                    if (existingVersion < Integer.valueOf(version)) {


                        db.insertWithOnConflict(TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
                        //Log.e(TAG, "In update: rows effected are:" + row);
                        Log.e(TAG, "Row was updated, for the key" + key + "And updated value is:" + value + "The latest version is:" + version);

                    } else {
                        Log.e(TAG, "Inside filterInsert, skipping, as a better version exists already" + lin);
                    }

                } else {
                    Log.e(TAG, "keyVal does not exists, inserting it" + lin);

                    // contentValues.put("version", "0");

                    // getContext().getContentResolver().insert(message_uri,contentValues);
                    //String version = "0";
                    //String[] args = {key, value, version};
                    db.insertWithOnConflict(TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
                    //db.execSQL("INSERT OR REPLACE INTO message_tbl (key, value,version) VALUES (?, ?,?)", args);
                    Log.e(TAG, "Filtered results insertion Inserting key:" + key + "value: " + value + "version" + version);
                    i++;
                    // }
                }
            }


        }
        Log.e(TAG, "Total Insertions for filtered is :" + i);
        return true;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean createNodeMapping() {
        try {
            for (String i : allPorts) {
                //Log.e(TAG,"createNodeMapping: for port i :"+i+"genhash is "+genHash(i));
                nodeMapping.put(genHash(i), i);
            }
            for (Map.Entry<String, String> entry : nodeMapping.entrySet()) {
                //Log.e(TAG,"Key: " + entry.getKey() + ". Value: " + entry.getValue());
            }

            return true;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No Such Algorithm");
        }
        return false;
    }

    private boolean isPresentAlready(String selection) {
        Log.e(TAG, "isPresentAlready for the key:" + selection);
        SQLiteDatabase db = db1.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        String newSelection = "key='" + selection + "'";
        Cursor cursor;
        cursor = queryBuilder.query(db, new String[]{"key", "value"}, newSelection,
                null, null, null, null);
        if (cursor == null) {
            return false;
        }
        if (cursor.getCount() == 0) {
            // Log.e(TAG, "isPresentAlready is FALSE for the key:" + selection);
            return false;
        } else {
            //Log.e(TAG, "isPresentAlready is TRUE fro the key: " + selection);
            return true;
        }

    }

    private String fetchQueryResults(String selection) {
        SQLiteDatabase db = db1.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        Cursor cursor;

        String newSelection = "key='" + selection + "'";
        //search key logic
        cursor = queryBuilder.query(db, new String[]{"key", "value"}, newSelection,
                null, null, null, null);

        return ConvertCursorToStringForKeyVal(cursor);
    }

    private String fetchAllRecords(String selection) {
        SQLiteDatabase db = db1.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        Cursor cursor;

        cursor = db.rawQuery("SELECT * FROM message_tbl", null);
        // Log.e(TAG, "Fetch all records count is: " + cursor.getCount());
        String finalResults = ConvertCursorToString(cursor);
        if (cursor != null) {
            Log.e(TAG, "fetchAllResults count is:" + cursor.getCount() + "String value of results is: " + finalResults);
        }
        return finalResults;
    }

    private String getNextNode(String key) {
        //give a hash value of port and get next port
        int index = Arrays.binarySearch(mapKeys, key);
        // Log.e(TAG,"Found index of key:" +index+"For the node:"+key);
        if (index != mapKeys.length - 1) {
            //  Log.e(TAG,"Found next node in IF:"+nodeMapping.get(mapKeys[index+1]));
            return (nodeMapping.get(mapKeys[index + 1]));
        } else {
            //  Log.e(TAG,"Found next node in ELSE:"+nodeMapping.get(mapKeys[0]));
            return (nodeMapping.get(mapKeys[0]));
        }


    }

    private String getPreviousNode(String key) {
        //give a hash value of port and get next port
        int index = Arrays.binarySearch(mapKeys, key);
        // Log.e(TAG, "Previous Node for key found at index:" + index);
        // Log.e(TAG,"Found index of key:" +index+"For the node:"+key);
        if (index == 0) {
            //  Log.e(TAG,"Found next node in IF:"+nodeMapping.get(mapKeys[index+1]));
            return (nodeMapping.get(mapKeys[4]));
        } else {
            //  Log.e(TAG,"Found next node in ELSE:"+nodeMapping.get(mapKeys[0]));
            return (nodeMapping.get(mapKeys[index - 1]));
        }


    }

    private String getNextToNextNode(String key) {
        //give a hash value of port and get next port
        int index = Arrays.binarySearch(mapKeys, key);
        //   Log.e(TAG,"Found index of key:" +index+"For the node:"+key);
        if (index <= 2) {
            // Log.e(TAG, "Found next to next node in IF:" + nodeMapping.get(mapKeys[index + 1]));
            return (nodeMapping.get(mapKeys[index + 2]));
        } else if (index == 3) {
            //Log.e(TAG,"Found next to next node in ELSE:"+nodeMapping.get(mapKeys[0]));
            return (nodeMapping.get(mapKeys[0]));
        } else if (index == 4) {
            // Log.e(TAG,"Found next to next node in ELSE:"+nodeMapping.get(mapKeys[1]));
            return (nodeMapping.get(mapKeys[1]));
        }
        return null;

    }

    private boolean createArrayOfSortedKeys(TreeMap<String, String> treeMap) {
        mapKeys = new String[treeMap.size()];
        int pos = 0;
        for (String key : treeMap.keySet()) {
            mapKeys[pos++] = key;
        }
        //array contains hashkeys in order
        Log.e(TAG, "Length of Array of Sorted Keys is" + mapKeys.length);
        return true;
    }

    private String getKeyLocation(String key) {
        try {
            String keyHash = genHash(key);
           /* Log.e(TAG,key+"  (keyHash.compareTo(mapKeys[0])>0 && (keyHash.compareTo(mapKeys[1])<0)"+(keyHash.compareTo(mapKeys[0])>0 && (keyHash.compareTo(mapKeys[1])<0)));
            Log.e(TAG,key+"  (keyHash.compareTo(mapKeys[1])>0 && (keyHash.compareTo(mapKeys[2])<0))"+(keyHash.compareTo(mapKeys[1])>0 && (keyHash.compareTo(mapKeys[2])<0)));
            Log.e(TAG,key+"  (keyHash.compareTo(mapKeys[2])>0 && (keyHash.compareTo(mapKeys[3])<0))"+(keyHash.compareTo(mapKeys[2])>0 && (keyHash.compareTo(mapKeys[3])<0)));
            Log.e(TAG,key+"  (keyHash.compareTo(mapKeys[3])>0 && (keyHash.compareTo(mapKeys[4])<0))"+(keyHash.compareTo(mapKeys[3])>0 && (keyHash.compareTo(mapKeys[4])<0)));
            Log.e(TAG,key+"  (keyHash.compareTo(mapKeys[4])>0 && (keyHash.compareTo(mapKeys[0])>0))"+(keyHash.compareTo(mapKeys[4])>0 && (keyHash.compareTo(mapKeys[0])>0)));
            Log.e(TAG,key+"  (keyHash.compareTo(mapKeys[4])<0 && (keyHash.compareTo(mapKeys[0])<0))"+(keyHash.compareTo(mapKeys[4])<0 && (keyHash.compareTo(mapKeys[0])<0)));
   */
            if (keyHash.compareTo(mapKeys[0]) < 0) {
                //  Log.e(TAG,"The key :"+key+" less than node 0 its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[0]));
            }
            if (keyHash.compareTo(mapKeys[4]) > 0) {
                //   Log.e(TAG,"The key :"+key+" greater than node 4 its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[0]));
            }
            if ((keyHash.compareTo(mapKeys[0]) > 0 && (keyHash.compareTo(mapKeys[1]) < 0))) {
                //  Log.e(TAG,"The key :"+key+" greater than node 0 and less than node 1, therefore it belongs to node 1, its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[1]));

            } else if ((keyHash.compareTo(mapKeys[1]) > 0 && (keyHash.compareTo(mapKeys[2]) < 0))) {
                // Log.e(TAG,"The key :+"+key+" greater than node 1 and less than node 2, therefore it belongs to node 2, its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[2]));

            } else if ((keyHash.compareTo(mapKeys[2]) > 0 && (keyHash.compareTo(mapKeys[3]) < 0))) {
                // Log.e(TAG,"The key:+"+key+" is greater than node 2 and less than node 3, therefore it belongs to node 3, its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[3]));

            } else if ((keyHash.compareTo(mapKeys[3]) > 0 && (keyHash.compareTo(mapKeys[4]) < 0))) {
                // Log.e(TAG,"The key:"+key+" is greater than node 3 and less than node 4, therefore it belongs to node 4, its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[4]));

            }
/*            else if((keyHash.compareTo(mapKeys[4])>0 && (keyHash.compareTo(mapKeys[0])>0))){
                Log.e(TAG,"The key is:"+key+" greater than node 4 and greater than node 0, therefore it belongs to node 0, its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[0]));

            }
            else if((keyHash.compareTo(mapKeys[4])<0 && (keyHash.compareTo(mapKeys[0])<0))){
                Log.e(TAG,"The key is:"+key+" less than node 4 and less than node 0, therefore it belongs to node 0, its hash is:"+keyHash);
                return (nodeMapping.get(mapKeys[0]));

            }*/
            else {
                Log.e(TAG, "We missed a case: key:" + keyHash);
            }

            return null;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No such Algo");
        }
        return null;
    }

    private int getVersion(String key) {
        db1 = new DBHelper(getContext());
        SQLiteDatabase db = db1.getWritableDatabase();
/*
        long count = DatabaseUtils.queryNumEntries(db,
                "message_tbl", "key = ?", new String[]{key});
        Log.e(TAG,"getVersion for the key:"+key+" is "+count);*/

        Cursor cursor;


        cursor = db.rawQuery("SELECT * FROM message_tbl where key like '" + key + "';", null);
        if (cursor == null) {
            return 0;
        }

        if (cursor.getCount() == 0) {
            return 0;
        }

        cursor.moveToFirst();
        int keyIndex = cursor.getColumnIndex("version");
        Log.e(TAG, "The version found is" + cursor.getString(keyIndex));
        return Integer.valueOf(cursor.getString(keyIndex));
    }

    //convert the cursor to string
    public String ConvertCursorToString(Cursor resultCursor) {
        String row = "";

        if (resultCursor != null && resultCursor.getCount() != 0) {
            resultCursor.moveToFirst();

            while (true) {
                int keyIndex = resultCursor.getColumnIndex(ID);
                int valueIndex = resultCursor.getColumnIndex(MESSAGE);
                int versionIndex = resultCursor.getColumnIndex("version");
                // Log.e(TAG, "KeyIndex is:" + keyIndex + "ValueIndex is: " + valueIndex);

                if (keyIndex == -1 || valueIndex == -1) {
                    Log.e(TAG, "Dude, something went wrong while converting cursor to string");
                    resultCursor.close();
                }

                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                String returnVersion = resultCursor.getString(versionIndex);

                row = row + returnKey.trim() + "," + returnValue.trim() + "," + returnVersion.trim() + "\n";
                // Log.e(TAG, "Intermediate row is: " + row);
                //continue while until last row encountered
                if (!resultCursor.isLast()) {
                    resultCursor.moveToNext();

                } else {
                    break;
                }
            }
        }
        //Log.e(TAG, "Output of ConvertCursorToString is: " + row);
        return row;
    }

    //convert the map to cursor
    public Cursor convertMapToCursor(Map<String, String> cursorMap) {
        //define a mutable cursor with key value as column
        String[] columns = {"key", "value"};

        MatrixCursor cursor = new MatrixCursor(columns);
        //for each entry put its keyy and value to this matrixcursor
        for (Map.Entry<String, String> entry : cursorMap.entrySet()) {

            Object[] rows = new Object[cursor.getColumnCount()];

            rows[cursor.getColumnIndex("key")] = entry.getKey().trim();
            rows[cursor.getColumnIndex("value")] = entry.getValue().split(",")[0];
            rows[cursor.getColumnIndex("version")] = entry.getValue().split(",")[1];

            cursor.addRow(rows);
        }

        return cursor;
    }

    //convert the string to map
    public Map<String, String> convertStringToMap(String finalResult) {
        // Log.e(TAG, "Inside convertStringToMap" + finalResult);
        HashMap<String, String> map = new HashMap<>();
        String[] lines = finalResult.split("\\r?\\n");
        //Log.e(TAG,"convertStringToMap"+lines[0]);
        for (String lin : lines) {
            //  Log.e(TAG, "lin: " + lin);
            if (lin.contains(",")) {
                String[] keyValue = lin.split(",");
/*                for (String i : keyValue) {
                    Log.e(TAG, "keyValue split into" + i);
                }*/
                map.put(keyValue[0].trim(), keyValue[1].trim() + "," + keyValue[2].trim());
            }

        }
        return map;
    }

    //convert the string to map
    public Map<String, String> convertStringToMapForKeyVal(String finalResult) {
        // Log.e(TAG, "Inside convertStringToMap" + finalResult);
        HashMap<String, String> map = new HashMap<>();
        String[] lines = finalResult.split("\\r?\\n");
        //Log.e(TAG,"convertStringToMap"+lines[0]);
        for (String lin : lines) {
            //  Log.e(TAG, "lin: " + lin);
            if (lin.contains(",")) {
                String[] keyValue = lin.split(",");
/*                for (String i : keyValue) {
                    Log.e(TAG, "keyValue split into" + i);
                }*/

                map.put(keyValue[0].trim(), keyValue[1].trim());


            }

        }
        return map;
    }

    public String ConvertCursorToStringForKeyVal(Cursor resultCursor) {
        String row = "";

        if (resultCursor != null && resultCursor.getCount() != 0) {
            resultCursor.moveToFirst();

            while (true) {
                int keyIndex = resultCursor.getColumnIndex(ID);
                int valueIndex = resultCursor.getColumnIndex(MESSAGE);
                // int versionIndex = resultCursor.getColumnIndex("version");
                // Log.e(TAG, "KeyIndex is:" + keyIndex + "ValueIndex is: " + valueIndex);

                if (keyIndex == -1 || valueIndex == -1) {
                    Log.e(TAG, "Dude, something went wrong while converting cursor to string");
                    resultCursor.close();
                }

                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                //String returnVersion = resultCursor.getString(versionIndex);

                row = row + returnKey.trim() + "," + returnValue.trim() + "\n";
                // Log.e(TAG, "Intermediate row is: " + row);
                //continue while until last row encountered
                if (!resultCursor.isLast()) {
                    resultCursor.moveToNext();

                } else {
                    break;
                }
            }
        }
        //Log.e(TAG, "Output of ConvertCursorToString is: " + row);
        return row;
    }

    public Cursor convertMapToCursorForKeyVal(Map<String, String> cursorMap) {
        //define a mutable cursor with key value as column
        String[] columns = {"key", "value"};

        MatrixCursor cursor = new MatrixCursor(columns);
        //for each entry put its keyy and value to this matrixcursor
        for (Map.Entry<String, String> entry : cursorMap.entrySet()) {

            Object[] rows = new Object[cursor.getColumnCount()];

            rows[cursor.getColumnIndex("key")] = entry.getKey().trim();
            if (entry.getValue().trim().contains(",")) {
                rows[cursor.getColumnIndex("value")] = entry.getValue().trim().split(",")[0];
            } else {
                rows[cursor.getColumnIndex("value")] = entry.getValue().trim();
            }


            cursor.addRow(rows);
        }

        return cursor;
    }

    private void deleteParticularRecord(String selection) {

        SQLiteDatabase db = db1.getWritableDatabase();
        int i = db.delete(TABLE_NAME, "key=?", new String[]{selection.toString()});
        Log.e(TAG, "Inside deleteParticularRecord, number of deleted records is:" + i);
    }

    private void deleteAll() {

        SQLiteDatabase db = db1.getWritableDatabase();
        db.execSQL("delete from " + TABLE_NAME);
    }

    private String filterKeyValues(String KeyValue, String AVDNumber) {
        String row = "";


        try {
            Map<String, String> tempMap = convertStringToMap(KeyValue);
            for (Map.Entry<String, String> entry : tempMap.entrySet()) {
                String currentKey = entry.getKey();
                String currentValueValue = entry.getValue();
                String currentValue = currentValueValue.split(",")[0];
                String currentVersion = currentValueValue.split(",")[1];


                String currentKeyLocation = getKeyLocation(currentKey);
                String nextNode = getNextNode(genHash(currentKeyLocation));
                String nextToNextNode = getNextToNextNode(genHash(currentKeyLocation));

                if (nextNode.equals(AVDNumber) || currentKeyLocation.equals(AVDNumber) || nextToNextNode.equals(AVDNumber)) {
                    //  Log.e(TAG,"Rows Belongs to portStr: "+portStr);
                    row = row + currentKey.trim() + "," + currentValue.trim() + "," + currentVersion.trim() + "\n";
                }
            }
            //    Log.e(TAG,"Output of filterKeyValues "+row);


            return row;
        } catch (NoSuchAlgorithmException e) {

        }
        return null;

    }

    private boolean isRecoveryMode() {

        isRecoveredFully = false;
        voteCount1 = 0;

        for (String i : allAvds) {
            if (!i.equals(portStr)) {
                //fetch all results from its next node
                Message fetchAllResultFromReplica1 = new Message();
                fetchAllResultFromReplica1.OriginPort = portStr;
                fetchAllResultFromReplica1.queryOrigin = myPort;
                fetchAllResultFromReplica1.type = 9;
                fetchAllResultFromReplica1.selection = "Recover";
                fetchAllResultFromReplica1.sendPort = Integer.toString(Integer.valueOf(i) * 2);
                Log.e(TAG, "In Recovery mode function: sending to all ports:" + fetchAllResultFromReplica1.sendPort + " and the key is" + fetchAllResultFromReplica1.selection);

                new Thread(new ClientThread(fetchAllResultFromReplica1)).start();
               // new RecoveryMessageClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fetchAllResultFromReplica1);
            }
        }

        return true;
    }

    private int getRowCount() {
        SQLiteDatabase db = db1.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        Cursor cursor;
        int i;

        cursor = db.rawQuery("SELECT key,value FROM message_tbl", null);
        // Log.e(TAG, "Fetch all records count is: " + cursor.getCount());
        if (cursor != null) {
            i = cursor.getCount();
        } else {
            return 0;
        }

        return i;
    }

    private boolean isTablePresentAlready() {


        File dbFile = getContext().getDatabasePath(DATABASE_NAME);
        return dbFile.exists();
    }


}