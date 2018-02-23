package misc;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public abstract class AddressUtils {
    public static InetSocketAddress parseAddress(String addressString) {
        int port;
        InetSocketAddress address;
        String[] addPort = addressString.split(":");
        if (addPort.length != 2) {
            return null;
        }
        try {
            port = Integer.parseInt(addPort[1]);
            address = new InetSocketAddress(addPort[0], port);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return address;
    }
    
    public static ArrayList<InetSocketAddress> parseAddresses(String addressStrings) {
        ArrayList<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        
        String[] addressStringsList = addressStrings.split(",");
        
        for (int i=0; i < addressStringsList.length; i++) {
            InetSocketAddress address = AddressUtils.parseAddress(addressStringsList[i]);
            if (address == null) {
                return null;
            }
            addresses.add(address);
        }
        
        return addresses;
    }
}
