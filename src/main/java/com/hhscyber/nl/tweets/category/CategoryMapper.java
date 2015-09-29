/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.category;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eve
 */
public class CategoryMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        context.write(row, resultToPut(row, value));
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
        Put put = new Put(key.get());
        byte[] b = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("keyword"));

        String cat = CategoryLookup(new String(b));
        for (KeyValue kv : result.raw()) {
            put.add(kv);
        }
        put.add(Bytes.toBytes("content"), Bytes.toBytes("category"), Bytes.toBytes(cat));
        return put;
    }

    private static String CategoryLookup(String keyword) {
        switch (keyword) {
            case "exploit":
            case "exploitdb":
            case "exploit-kit":
            case "exploits":
            case "heartbleed":
            case "metasploit":
            case "poodle-attack":
            case "beast-attack":
            case "privilege-escalation":
            case "seh-chain":
            case "shellshock":
                return "exploit";

            case "adscam":
            case "clickfraud":
            case "clickjacking":
            case "hoaxes":
            case "logicbomb":
            case "pharming":
            case "phising":
            case "spam":
            case "scam":
            case "malicious":
            case "malvertisement":
            case "malvertising":
                return "scam";

            case "antivirus":
            case "bug-bounties":
            case "counterintelligence":
            case "counter-intelligence":
            case "firewall":
            case "microsoft-security":
            case "non-repudiation":
            case "patchtuesday":
            case "pentest":
            case "plaintext":
            case "software-patch":
            case "virus-definitions":
            case "infosec":
            case "ict-security":
                return "security";

            case "datamining":
            case "exfiltration":
            case "keylogger":
            case "packetsniffer":
            case "portscan":
            case "sniffing":
            case "spillage":
            case "warchalking":
            case "wardialing":
            case "wardriving":
            case "cryptoanalysis":
            case "spoofing":
                return "gathering";

            case "drive-by-attack":
            case "ddos":
            case "denial-of-service":
            case "dnspoison":
            case "dos":
            case "flooding":
            case "syn-flood":
            case "tsunami-flood":
            case "dnsspoofing":
            case "mitm":
            case "mitma":
            case "bruteforce":
            case "rainbow-table":
                return "attack";

            case "radiojammer":
            case "radio-jammer":
            case "wifijammer":
            case "wifi-jammer":
            case "blackhat":
            case "black-hat":
            case "celljammer":
            case "cell-jammer":
            case "certifi-gate":
            case "grayhat":
            case "gray-hat":
            case "hacked":
            case "hackingteam":
            case "intrusion":
            case "jailbreak":
            case "mapping":
            case "remotecontrol":
            case "securitybreach":
            case "whitehat":
            case "white-hat":
            case "code-injection":
            case "sessionhijack":
            case "socialengineering":
            case "tampered":
                return "hacking";

            case "bufferoverflow":
            case "codebreaking":
            case "common-vulnerabilities":
            case "cookiestuffing":
            case "cross-site-request-forgery":
            case "cve":
            case "dataleakage":
            case "exposure":
            case "flaweddesign":
            case "hardcoded":
            case "hard-coded":
            case "kernel-vulnerability":
            case "memoryleak":
            case "mitigation":
            case "security-flaws":
            case "securityhole":
            case "securityissue":
            case "sqlinjection":
            case "vulnerability":
            case "vulnerability-database":
            case "vuln-reports":
            case "weakness":
            case "xss":
            case "0day":
            case "zeroday":
            case "databreach":
            case "shellcode":
            case "backdoor":
                return "vulnerability";

            case "computervirus":
            case "macrovirus":
            case "trojan":
            case "virus":
            case "bitcoinminer":
            case "cryptolocker":
            case "cryptovirus":
            case "ransomware":
            case "worm":
            case "stuxnet":
            case "zeusbot":
            case "botmaster":
            case "scareware":
            case "spyware":
            case "grayware":
            case "adware":
            case "rootkit":
            case "malware":
                return "virus";

            case "botnet":
            case "zombiepc":
            case "cybersecurity":
            case "cyber":
            case "cybercrime":
            case "cyberhack":
            case "cyberincident":
            case "cyberthreat":
            case "cyberwar":
            case "honeypot":
            case "identitytheft":
            case "information-stealing":
            case "unauthorized":
                return "cybercrime";

            default:
                return "other";
        }
    }
}
