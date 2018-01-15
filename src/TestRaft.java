import java.net.InetSocketAddress;

public class TestRaft {

    public static void main(String[] args) {
        InetSocketAddress[] list1 = {new InetSocketAddress ("localhost", 6061)};
        InetSocketAddress[] list2 = {new InetSocketAddress ("localhost", 6060)};
        Server server1 = new Server(new InetSocketAddress ("localhost", 6060),  list1, "Server1");
        Server server2 = new Server(new InetSocketAddress ("localhost", 6061),  list2, "Server2");
        (new Thread(server1)).start();
        (new Thread(server2)).start();
    }

}
