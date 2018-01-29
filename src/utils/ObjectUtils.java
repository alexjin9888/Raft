package utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ObjectUtils {

    public static byte[] serializeObject(Serializable object) throws IOException {
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
    
    public static Object deserializeObject(byte[] objectBytes) throws IOException {
        Object object = null;

        ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
        
        ObjectInputStream in = new ObjectInputStream(bis);
        try {
            object = in.readObject();
        } catch (ClassNotFoundException e) {
            assert(false);
        }

        in.close();
        bis.close();
        
        return object;
    }

}
