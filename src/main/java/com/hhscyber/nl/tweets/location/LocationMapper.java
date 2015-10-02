/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.location;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hhscyber.nl.carmen.LocationResolver;
import com.hhscyber.nl.carmen.types.Location;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minidev.json.JSONObject;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class LocationMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        hbaseHadoop(row, value);
        //Put p = resultToPut(row, value);
//        if (p != null) {
//            context.write(row, p);
//        }
    }
    
    /*
     Read json directly 
    */
    private static void normalHadoop(){
        
    }
    
    /** 
     * Trick to not modify existing code/might be slower though
     * @param key
     * @param result 
     */
    private static void hbaseHadoop(ImmutableBytesWritable key, Result result)
    {
        byte[] b = getValueSafe(result,"profile","utc_offset");
        byte[] b2 = getValueSafe(result,"profile","location");
        byte[] b3 = getValueSafe(result,"content","geo");
        byte[] b4 = getValueSafe(result,"profile","geo_enabled");
        /*
            
            PLACE object skipped
            USER object - GEO_enabled - LOCATION - utc_offset
            Root object geo code
            
            */
        JSONObject json = new JSONObject();
        String offset = createStringFromByte(b);
        String location = createStringFromByte(b2);
        String geo = createStringFromByte(b3);
        String geo_enabled  = createStringFromByte(b4);
        JSONObject user = new JSONObject();
        JSONObject entities = new JSONObject();
        entities.put("utc_offset", offset);
        user.put("location",location);
        user.put("geo_enabled",geo_enabled);
        json.put("user",user);
        json.put("geo",geo);
        json.put("entities",entities);
        try {
            getLocation(json);
        } catch (IOException ex) {
            Logger.getLogger(LocationMapper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    /**
     * Stop nullpointers
     * @param b
     * @return 
     */
    private static String createStringFromByte(byte[] b){   
        if(b!= null){
            return new String(b);
        }
        else{
            return "";
        }
    }
    
    private static byte[] getValueSafe(Result result,String family, String column){
        if(result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))){
            return result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
        }
        else{
            return null;
        }
    }

     private static void getLocation(JSONObject json) throws IOException{
        // Parse the command line.
        LocationResolver resolver = LocationResolver.getLocationResolver();

        int numResolved;
        int total;
        ObjectMapper mapper = new ObjectMapper();
        numResolved = 0;
        total = 0;

        @SuppressWarnings("unchecked")
        HashMap<String, Object> tweet = (HashMap<String, Object>) mapper.readValue(json.toString(), Map.class);

        total++;
        Location location = resolver.resolveLocationFromTweet(tweet);

        if (location != null) {
            System.out.println("Found location: " + location.toString());
            numResolved++;
        }

         System.out.println("Resolved locations for " + numResolved + " of " + total + " tweets.");
    }

    
//    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
//        Put put = new Put(key.get());
//        byte[] b = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("lang"));
//        byte[] b2 = result.getValue(Bytes.toBytes("profile"), Bytes.toBytes("lang"));
//
//        String clang = new String(b);
//        String plang = new String(b2);
//
//        System.out.println("CLANG " + clang + " PLANG " + plang);
//        if (clang.equals("en") || plang.equals("en") || clang.equals("nl") || plang.equals("nl")) {
//            for (KeyValue kv : result.raw()) {
//                put.add(kv);
//            }
//            return put;
//        } else {
//            return null;
//        }
//
//    }
}
