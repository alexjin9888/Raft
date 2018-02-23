package misc;

import java.net.InetSocketAddress;

public abstract class AddressUtils {
    public static InetSocketAddress parse(String address) {
        int port;
        InetSocketAddress serverAddress;
        String[] addPort = address.split(":");
        if (addPort.length != 2) {
            return null;
        }
        try {
            port = Integer.parseInt(addPort[1]);
            serverAddress = new InetSocketAddress(addPort[0], port);
        } catch (Exception e) {
            return null;
        }
        return serverAddress;
    }
}
