import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Timer;

public class RaftClient implements SerializableReceiver.Handler {
    
    private InetSocketAddress myAddress;
    
    private ArrayList<InetSocketAddress> serverAddresses;
    
    private Timer myTimer;
        
    private SerializableSender serializableSender;
    
    public RaftClient(InetSocketAddress myAddress, ArrayList<InetSocketAddress> serverAddresses) {
        this.myAddress = myAddress;
        this.serverAddresses = serverAddresses;
        
        serializableSender = new SerializableSender();
        
        myTimer = new Timer();
        
        Scanner reader = new Scanner(System.in);
        
        (new Thread() {
            public void run() {
                while(true) {
                    String command = reader.nextLine();
                    System.out.println(command);
                }
            }
        }).start();
        
        SerializableReceiver serializableReceiver =
                new SerializableReceiver(myAddress, this);
    }
    
    public synchronized void handleSerializable(Serializable object) {
    }
    
    public static void main(String[] args) {
        RaftClient myClient = new RaftClient(new InetSocketAddress("localhost", 6070), new ArrayList<InetSocketAddress>());
    }
}

