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
public abstract class ObjectUtils {
    
    public static Serializable deepClone(Serializable obj) {
        return ObjectUtils.deserialize(ObjectUtils.serialize(obj));
    }

    // TODO: make serialize/deserialize private after we come up with
    // a new data format that is human-readable for persistent state storage.
    /**
     * Serializes an serializable object into a sequence of bytes.
     * @param object       object to be serialized
     * @return             a byte array representing the serialized object
     */
    public static byte[] serialize(Serializable object) {
        byte[] objectBytes;
                
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            oos.flush();
            objectBytes = bos.toByteArray();
        } catch (IOException e) {            
            objectBytes = null;
        }
        
        return objectBytes;
    }
    
    /**
     * Deserializes a sequence of bytes into a serializable object.
     * @param objectBytes  a byte array to be deserialized
     * @return             an object created from deserializing the byte array
     */
    public static Serializable deserialize(byte[] objectBytes) {
        Object object;


        try (ByteArrayInputStream bais = new ByteArrayInputStream(objectBytes);
                 ObjectInputStream ois = new ObjectInputStream(bais)) {
            object = ois.readObject();
        } catch (ClassNotFoundException e) {
            // TODO: figure out proper error-handling for this
            // also log something that is informative
            object = null;
            e.printStackTrace();
        } catch (IOException e) {
            // TODO see todo above
            object = null;
        }
        
        return (Serializable) object;
    }

}
