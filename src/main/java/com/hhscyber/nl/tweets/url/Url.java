/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.url;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dev
 */
public class Url {

    public StringBuilder execute(String url)
    {
        try {
            URL web = new URL(url);
            URLConnection yc = web.openConnection();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            yc.getInputStream()));
            String inputLine;
            
            StringBuilder build = new StringBuilder();
            
            while ((inputLine = in.readLine()) != null){
                build.append(inputLine);
            }
            in.close();
            return build;
        } catch (MalformedURLException ex) {
            Logger.getLogger(Url.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Url.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
