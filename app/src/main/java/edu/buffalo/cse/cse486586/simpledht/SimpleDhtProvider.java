package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.NetworkOnMainThreadException;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private int thisPort;
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private String portStr;
    private String nodeID;
    private String succID;
    private String predID;
    private String succ;
    private String pred;
    private String maxID;
    private String minID;
    private HashMap<String, String> mapping; /// 5554's mapping of portStr to hashed versions
    private HashMap<String, String> records;
    private Boolean isSolo = false;
    private ArrayList<String> id_list;

    class Bundle{
        private String command;
        private String key;
        private String value;
        private ArrayList<Dub> pairs;
        Bundle(String com, String k, String val, ArrayList<Dub> d){
            command = com;
            key = k;
            value = val;
            pairs = d;

        }
    }

    class Dub{
        private String k;
        private String v;
        Dub(String key, String value){
            k = key;
            v = value;
        }
    }

    @Override
    public boolean onCreate() {
        //not totally sure if should implement here but w/e
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        mapping = new HashMap<String, String>();
        records = new HashMap<String, String>();
        id_list = new ArrayList<String>();
        try{
            nodeID = genHash(portStr);
            id_list.add(nodeID);
            maxID = nodeID;
            minID = nodeID;
        }
        catch(NoSuchAlgorithmException e){
            Log.e(TAG, e.getMessage());
        }
        if(!portStr.equals("5554")) {
            try {
                // put non 5554 joins here
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("10008"));
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(new Bundle("join", nodeID, portStr, null));
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                in.readLine();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, e.getMessage());
            } catch (IOException e) {
                isSolo = true;
                Log.e(TAG, e.getMessage());
            }
            catch (NetworkOnMainThreadException e){
                Log.i(TAG, "BUG WHEN TRYNA CONNECT TO 5554");
                isSolo = true;
            }
        }
        else{
            isSolo = true;
            Log.i(TAG, "EVERYONE THINKS THEY ARE 5554");

        }
        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch(IOException e){
            Log.e(TAG, "CAN'T CREATE SERVER SOCKET");
        }
        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String command = "insert";
        String key = values.getAsString("key");
        //Log.i(TAG, "Will everything break?");
        String value = values.getAsString("value");
        if(isSolo){
            insertOG(key, value);
            return uri;
        }
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, command, key, value);
        return uri;
    }

    public void insertOG(String key, String value) {
        // TODO Auto-generated method stub
        records.put(key, value);
        value = value + "\n";
        FileOutputStream fos;
        try {
            // changed this to be genHash(key)
            fos = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "File write failed");
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(isSolo){
            // need to implement delete!
            deleteOG(selection);
        }
        String command = "delete";
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, command, selection);
        // TODO Auto-generated method stub
        return 0;
    }

    public int deleteOG(String selection) {
        // TODO Auto-generated method stub
        try {
            if (selection.equals("@") || selection.equals("*")) {
                // following iteration idiom obtained from https://stackoverflow.com/questions/1066589/iterate-through-a-hashmap
                for (String key : records.keySet()) {
                    getContext().deleteFile(key);
                }
                records.clear();
                return 0;
            }
        }
        catch(NullPointerException e){
            Log.e(TAG, e.getMessage());
        }
        records.remove(selection);
        try {
            getContext().deleteFile(selection);
        }
        catch(NullPointerException e){
            Log.e(TAG, e.getLocalizedMessage());
        }
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        if(isSolo) {
            return queryOG(selection);
        }
        // How to return????? Apparently this works??????
        String command = "query";
        AsyncTask task = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, command, selection, nodeID);
        try {
            return (Cursor)task.get();
        }
        catch(InterruptedException e){
            Log.e(TAG, "Query error" + e.getMessage());
        }
        catch(ExecutionException e){
            Log.e(TAG, "Query error" + e.getMessage());
        }
        return null;
    }

    public Cursor queryOG(String selection){
        // maybe close the streams if something's wrong?
        String[] columnNames = {"key", "value"};
        MatrixCursor mcursor = new MatrixCursor(columnNames);
        try{
            if (selection.equals("@") || selection.equals("*")) {
                for (String key : records.keySet()) {
                    MatrixCursor.RowBuilder rowBuilder = mcursor.newRow();
                    FileInputStream fis = new FileInputStream(getContext().getFileStreamPath(key));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                    String msg = reader.readLine();
                    rowBuilder.add("key", key);
                    rowBuilder.add("value", msg);
                }
                return mcursor;
            }
        }
        catch(IOException e){
            Log.e(TAG, "query IOexception:  " + e.getMessage());
        }
        MatrixCursor.RowBuilder rowBuilder = mcursor.newRow();
        try {
            Log.i(TAG, "key passed:" + selection);
            FileInputStream fis = new FileInputStream(getContext().getFileStreamPath(selection));
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            String msg = reader.readLine();
            rowBuilder.add("key", selection);
            rowBuilder.add("value", msg);
        }
        catch (IOException e){
            Log.e(TAG, "query IOexception:" + e.getMessage());
        }
        return mcursor;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private boolean isMinID(){
        return (isSolo || (nodeID.compareTo(predID) < 0));
    }

    // Begin asynchronous handling :-)

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        protected Void doInBackground(ServerSocket... sockets) {
            while(true){
                try{
                    ServerSocket serverSocket = sockets[0];
                    Socket clisco = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(clisco.getInputStream());
                    Bundle bun = (Bundle)ois.readObject();
                    String key = bun.key;
                    String hashedKey = genHash(bun.key);
                    String value = bun.value;
                    String command = bun.command;
                    ArrayList<Dub> pairs = bun.pairs;
                    if(command.equals("join")){
                        isSolo = false;
                        PrintWriter pw = new PrintWriter(clisco.getOutputStream());
                        pw.write("acknowledgement!");
                        ois.close();
                        pw.close();
                        clisco.close();
                        if(!portStr.equals("5554")){
                            Log.e(TAG, "Somehow a node besides 5554 was contacted for join");
                        }
                        mapping.put(hashedKey, value);
                        String successor = maxID;
                        String predecessor = minID;
                        if(hashedKey.compareTo(maxID) > 0){
                            predecessor = maxID;
                            successor = minID;
                            succID = minID;
                            maxID = hashedKey;
                        }
                        else if(hashedKey.compareTo(minID) < 0){
                            predecessor = maxID;
                            successor = minID;
                            maxID = minID;
                            minID = hashedKey;
                        }
                        else {
                            for(int i = 0; i < id_list.size(); i++) {
                                String temp = id_list.get(i);
                                if (temp.compareTo(predecessor) > 0 && temp.compareTo(hashedKey) < 0) {
                                    predecessor = temp;
                                }
                                if(temp.compareTo(successor) < 0 && temp.compareTo(hashedKey) > 0){
                                    successor = temp;
                                }
                            }
                        }
                        successor = mapping.get(successor);
                        predecessor = mapping.get(predecessor);
                        // check above for logic errors
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                ((Integer.parseInt(predecessor) * 2))); // connecting to newnode's pred
                        Bundle bung = new Bundle("update", nodeID, value, null); //value is actually newnode's portStr
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream gasp = new ObjectInputStream(socket.getInputStream());
                        oos.writeObject(bung);
                        Bundle vom = (Bundle) gasp.readObject();
                        gasp.close();
                        oos.close();
                        socket.close();
                        Socket redir = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                ((Integer.parseInt(value) * 2)));
                        ObjectOutputStream scooby = new ObjectOutputStream(redir.getOutputStream());
                        BufferedReader doo = new BufferedReader(new InputStreamReader(redir.getInputStream()));
                        scooby.writeObject(vom);
                        doo.readLine();
                        scooby.close();
                        doo.close();
                        redir.close();
                        Socket epi = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                ((Integer.parseInt(successor) * 2)));
                        ObjectOutputStream lorin = new ObjectOutputStream(epi.getOutputStream());
                        BufferedReader greg = new BufferedReader(new InputStreamReader(epi.getInputStream()));
                        lorin.writeObject(new Bundle("succdate", key, value, null));
                        greg.readLine();
                        lorin.close();
                        greg.close();
                        epi.close();

                    } else if (command.equals("update")) {
                        String newPort = value;
                        String newNode = hashedKey;
                        succ = newPort;
                        succID = value;
                        Bundle beavis = new Bundle("populate", succ, pred, new ArrayList<Dub>());
                        for(String keyboi : records.keySet()){
                            if (keyboi.compareTo(succID) > 0) {
                                beavis.pairs.add(new Dub(keyboi, records.get(keyboi)));
                                deleteOG(keyboi);
                            }
                        }
                        ObjectOutputStream oos = new ObjectOutputStream(clisco.getOutputStream());
                        oos.writeObject(beavis);
                        ois.close();
                        oos.close();
                        clisco.close();
                    } else if (command.equals("populate")){
                        succ = key;
                        succID = hashedKey;
                        pred = value;
                        predID = genHash(value);
                        for(Dub pair : pairs){
                            insertOG(pair.k, pair.v);
                        }
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(clisco.getOutputStream()));
                        out.write("acknowledgement!");
                        ois.close();
                        out.close();
                        clisco.close();

                    } else if (command.equals("succdate")){
                        pred = value;
                        predID = key;
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(clisco.getOutputStream()));
                        out.write("acknowledgement!");
                        ois.close();
                        out.close();
                        clisco.close();
                    }
                    else if (command.equals("insert")) {
                        if (hashedKey.compareTo(nodeID) <= 0 && hashedKey.compareTo(predID) > 0) {
                            insertOG(hashedKey, value);
                        } else if (isMinID() && hashedKey.compareTo(maxID) > 0) {
                            insertOG(hashedKey, value);
                        } else {
                            try {
                                PrintWriter pw = new PrintWriter(clisco.getOutputStream());
                                pw.write("acknowledgement!");
                                ois.close();
                                pw.close();
                                clisco.close();
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(succ));
                                Bundle bundle = new Bundle(command, hashedKey, value, null);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                oos.writeObject(bundle);
                                in.readLine();
                                oos.close();
                                in.close();
                                socket.close();
                            } catch (IOException e) {
                                Log.e(TAG, e.getMessage());
                            }
                        }
                    } else if (command.equals("query")) {
                        if (hashedKey.compareTo(nodeID) <= 0 && hashedKey.compareTo(predID) > 0) {
                            queryOG(key);
                        } else if (isMinID() && hashedKey.compareTo(maxID) > 0) {
                            queryOG(key);
                        } else {
                            try {
                                PrintWriter pw = new PrintWriter(clisco.getOutputStream());
                                pw.write("acknowledgement!");
                                ois.close();
                                pw.close();
                                clisco.close();
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(succ));
                                Bundle bundle = new Bundle(command, key, null, null);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                oos.writeObject(bundle);
                                in.readLine();
                                oos.close();
                                in.close();
                                socket.close();
                            } catch (IOException e) {
                                Log.e(TAG, e.getMessage());
                            }
                        }
                    } else if (command.equals("delete")) {
                        if (hashedKey.compareTo(nodeID) <= 0 && hashedKey.compareTo(predID) > 0) {
                            deleteOG(key);
                        } else if (isMinID() && hashedKey.compareTo(maxID) > 0) {
                            deleteOG(key);
                        } else {
                            try {
                                PrintWriter pw = new PrintWriter(clisco.getOutputStream());
                                pw.write("acknowledgement!");
                                ois.close();
                                pw.close();
                                clisco.close();
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(succ));
                                Bundle bundle = new Bundle(command, key, null, null);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                oos.writeObject(bundle);
                                in.readLine();
                                oos.close();
                                in.close();
                                socket.close();
                            } catch (IOException e) {
                                Log.e(TAG, e.getMessage());
                            }
                        }
                    }

                }
                catch(IOException e){
                    Log.e(TAG, e.getMessage());
                }
                catch(ClassNotFoundException e){
                    Log.e(TAG, e.getLocalizedMessage());
                }
                catch(NoSuchAlgorithmException e){
                    Log.e(TAG, e.getLocalizedMessage());

                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        // Currently assuming that msgs will hold both the command and values passed
        protected Void doInBackground(String... msgs) {
            String command = msgs[0];
            String key = msgs[1];
            String hashedKey = null;
            try {
                hashedKey = genHash(msgs[1]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if(command.equals("insert")){
                String value = msgs[2];
                if(hashedKey.compareTo(nodeID) <= 0 && hashedKey.compareTo(predID) > 0){
                    insertOG(hashedKey, value);
                }
                else if(isMinID() && hashedKey.compareTo(maxID) > 0){
                    insertOG(hashedKey, value);
                }
                else{
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succ));
                        Bundle bundle = new Bundle(command, key, value, null);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        oos.writeObject(bundle);
                        in.readLine();
                        oos.close();
                        in.close();
                        socket.close();
                    }
                    catch(IOException e){
                        Log.e(TAG, e.getMessage());
                    }
                }
            }
            else if(command.equals("query")){
                try {
                    if(hashedKey.equals(genHash("@"))){
                        queryOG(key);
                    }
                    else if(hashedKey.equals(genHash("*"))){
                        queryOG(key);
                    }
                    else if(hashedKey.compareTo(nodeID) <= 0 && hashedKey.compareTo(predID) > 0){
                        queryOG(key);
                    }
                    else if(isMinID() && hashedKey.compareTo(maxID) > 0){
                        queryOG(key);
                    }
                    else{
                        try{
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succ));
                            Bundle bundle = new Bundle(command, key, null, null);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            oos.writeObject(bundle);
                            in.readLine();
                            oos.close();
                            in.close();
                            socket.close();
                        }
                        catch(IOException e){
                            Log.e(TAG, e.getMessage());
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            else if(command.equals("delete")){
                try {
                    if(hashedKey.equals(genHash("@"))){
                        deleteOG(key);
                    }
                    else if(hashedKey.compareTo(nodeID) <= 0 && hashedKey.compareTo(predID) > 0){
                        deleteOG(key);
                    }
                    else if(isMinID() && hashedKey.compareTo(maxID) > 0){
                        deleteOG(key);
                    }
                    else{
                        try{
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succ));
                            Bundle bundle = new Bundle(command, key, null, null);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            oos.writeObject(bundle);
                            in.readLine();
                            oos.close();
                            in.close();
                            socket.close();
                        }
                        catch(IOException e){
                            Log.e(TAG, e.getMessage());
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

}

