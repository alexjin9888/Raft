package misc;

import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * Utility class that provides some functions for parsing address string(s).
 */
public abstract class AddressUtils {
    /**
     * Parses an address string into an InetSocketAddress object.
     * @param addressString address string to be parsed.
     * @return an InetSocketAddress object representing the given address
     * string.
     */
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

    /**
     * Parses a list of address strings into a list of InetSocketAddress
     * objects.
     * @param addressStrings a list of address strings to be parsed.
     * @return a list of InetSocketAddress objects representing the given
     * list of address strings.
     */
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
