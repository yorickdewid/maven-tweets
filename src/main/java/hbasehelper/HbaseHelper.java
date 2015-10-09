/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasehelper;

import java.math.BigInteger;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author dev
 */
public class HbaseHelper {
    
    /**
     * Convert retrieved bytes to string
     * @param b
     * @return 
     */
    public static String createStringFromByte(byte[] b) {
        if (b != null) {
            return new String(b);
        } else {
            return "";
        }
    }

    /**
     * Convert retrieved bytes to integer
     * @param b
     * @return 
     */
    public static int createIntegerFromByte(byte[] b) {
        if (b != null) {
            int count = new BigInteger(b).intValue();
            return count;
        } else {
            return 0;
        }
    }

    /**
     * Byte values
     * @param result
     * @param family
     * @param column
     * @return 
     */
    public static byte[] getValueSafe(Result result, String family, String column) {
        if (result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))) {
            return result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
        } else {
            return null;
        }
    }
    
    public static byte[] getPutBytesSafe(String s)
    {
        if(s != null)
        {
            return Bytes.toBytes(s);
        }
        else{
            return Bytes.toBytes("");
        }
    }
    
    public static byte[] getPutBytesSafe(Boolean b)
    {
        if(b == true)
        {
            return Bytes.toBytes("true");
        }
        else{
            return Bytes.toBytes("false");
        }
    }
    
    public static String createStringFromRawHbase(Result result,String family,String column){
        byte[] b = getValueSafe(result, family, column);
        String s = createStringFromByte(b);
        return s;
    }
    
}
