package utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * ObjectUtils class to help with object serialization and deserialization
 *
 */
public class ObjectUtils {

    /**
     * @param object       object to be serialized
     * @return             a byte array containing the serialized object
     * @throws IOException
     */
    public static byte[] serializeObject(Serializable object) 
        throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        byte[] objectBytes = null;
        out = new ObjectOutputStream(bos);
        out.writeObject(object);
        out.flush();
        objectBytes = bos.toByteArray();
        out.close();
        bos.close();
        
        return objectBytes;
    }
    
    /**
     * @param objectBytes  a byte array to be deserialized
     * @return             an object containing the deserialized byte array
     * @throws IOException
     */
    public static Object deserializeObject(byte[] objectBytes) 
        throws IOException {
        Object object = null;

        ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
        
        ObjectInputStream in = new ObjectInputStream(bis);
        try {
            object = in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        in.close();
        bis.close();
        
        return object;
    }

}
