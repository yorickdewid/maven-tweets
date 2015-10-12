/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.locationcount;

/**
 *
 * @author dev
 */
public class countObject {
    public int knownLocations = 0;
    public int unknownLocations = 0;
    public int totalLocations = 0;

    @Override
    public String toString() {
        String s = "Known locations : "+this.knownLocations;
        s += "\n\n";
        s += "Unknown Locations : " + this.unknownLocations;
        s += "\n\n";
        s += "Total Locations " + this.totalLocations;
        return s;
    }
    
    
}
