package cn.sina.dsp.jedisproxy;

import java.io.UnsupportedEncodingException;
/**
 * @author bxeldor
 */
public class Util {

    public static String toStr(byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Error byte[] to String => " + e);
        }
    }
    public static byte[] toBytes(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Error serializing String:" + str + " => " + e);
        }
    }

}
