package utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Helper class for object serialization and deserialization.
 */
public abstract class SerializationUtils {

    /**
     * Serializes an serializable object into a sequence of bytes.
     * @param object       object to be serialized
     * @return             a byte array representing the serialized object
     * @throws IOException
     */
    public static byte[] serialize(Serializable object) 
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
     * Deserializes a sequence of bytes into a serializable object.
     * @param objectBytes  a byte array to be deserialized
     * @return             an object created from deserializing the byte array
     * @throws IOException
     */
    public static Serializable deserialize(byte[] objectBytes) 
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
        
        return (Serializable) object;
    }

}
