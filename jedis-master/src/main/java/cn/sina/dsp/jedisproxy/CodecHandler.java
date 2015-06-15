package cn.sina.dsp.jedisproxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bxeldor
 */
public class CodecHandler {
    public static final <T extends Serializable> byte[] encode(T obj) {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bout);
            out.writeObject(obj);
            bytes = bout.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error serializing object" + obj + " => " + e);
        }
        return bytes;
    }

    public static byte[] encode(String str) {
        return Util.toBytes(str);
    }
    
    public static byte[] encode(Number value) {
        return encode(String.valueOf(value));
    }

    public static String toStr(byte[] data) {
        return Util.toStr(data);
    }
    
  
   public static final List<String> toString(byte[]... byteslist){
       List<String> list = new ArrayList<String>(byteslist.length);
       for(byte[] b : byteslist) {
           if (b == null) {
               list.add(null);
           } else {
               list.add(toStr(b));
           }
       }
       return list;
   }
    
    
}
