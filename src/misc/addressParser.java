package misc;

import java.net.InetSocketAddress;

public abstract class addressParser {
    public static InetSocketAddress parse(String address) {
        int port;
        InetSocketAddress serverAddress;
        String[] addPort = address.split(":");
        if (addPort.length != 2) {
            throw new InvalidArgumentException("Unable to parse: "+address);
        }
        try {
            port = Integer.parseInt(addPort[1]);
            serverAddress = new InetSocketAddress(addPort[0], port);
        } catch (Exception e) {
            throw new InvalidArgumentException(e.getMessage()+
                    "\nUnable to parse: "+address);
        }
        return serverAddress;
    }
}
