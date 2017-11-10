package frequentAlgorithms;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringTokenizer;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class AprioriRandom{
    public static void main(String[] args) throws IOException{
        double s = 0.005;
        double num = 94512;
        Hashtable<String, Integer>singletons = new Hashtable<String, Integer>();
        Hashtable<String, Integer>doubletons = new Hashtable<String, Integer>();
        BufferedReader bf = new BufferedReader(new FileReader("shakespeare-basket"));
        String line;
        List<String> record = new ArrayList<String>();
        System.out.println("Now recording");
        while((line = bf.readLine()) != null){
            record.add(line);
        }
        Collections.shuffle(record);
        List<String> selected = new ArrayList<String>();
        selected = record.subList(0, (int)num);
        System.out.println("Now generating singletons...");
        for(String temp: selected){
            StringTokenizer words = new StringTokenizer(temp);
            while(words.hasMoreTokens()){
                String word = words.nextToken();
                if(singletons.containsKey(word)){
                    singletons.put(word, singletons.get(word)+1);
                }
                else{
                    singletons.put(word, 1);
                }
            }
        }
        System.out.println("In total " + singletons.size() + " candidates");
        System.out.println("Now cutting singletons...");
        for(Iterator<Map.Entry<String, Integer>>it = singletons.entrySet().iterator(); it.hasNext(); ){
            Map.Entry<String, Integer>temp = it.next();
            if(temp.getValue()/num < s){
                it.remove();
            }
        }
        System.out.println("In total " + singletons.size() + " singletons");
        System.out.println("Now generating doubletons...");
        for(String temp: selected){
            StringTokenizer words = new StringTokenizer(temp);
            List<String> candidates = new ArrayList<String>();
            while(words.hasMoreTokens()){
                String word = words.nextToken();
                if(singletons.containsKey(word)){
                    candidates.add(word);
                }
            }
            for(String key_one: candidates){
                for(String key_two: candidates){
                    if(key_one.compareTo(key_two) < 0){
                        if(doubletons.containsKey(key_one+","+key_two)){
                            doubletons.put(key_one+","+key_two, doubletons.get(key_one+","+key_two)+1);
                        }
                        else{
                            doubletons.put(key_one+","+key_two, 1);
                        }
                    }
                }
            }
        }
        System.out.println("In total " + doubletons.size() + " candidates");
        System.out.println("Now cutting doubletons...");
        for(Iterator<Map.Entry<String, Integer>>it = doubletons.entrySet().iterator(); it.hasNext(); ){
            Map.Entry<String, Integer>temp = it.next();
            if(temp.getValue()/num < s){
                it.remove();
            }
        }
        System.out.println("In total " + doubletons.size() + " doubletons");
        System.out.println("Now sorting...");
        ArrayList<Map.Entry<String, Integer>> sorted_doubletons = new ArrayList(doubletons.entrySet());
        Collections.sort(sorted_doubletons, new Comparator<Map.Entry<String, Integer>>(){
            public int compare(Map.Entry<String, Integer>pair_one, Map.Entry<String, Integer>pair_two){
                return pair_two.getValue().compareTo(pair_one.getValue());
            }
        });
        BufferedWriter bw = new BufferedWriter(new FileWriter("outputRandomFull.txt"));
        int i = 1;
        for(Map.Entry<String, Integer>pair: sorted_doubletons){
            bw.write(pair.getKey() + "\t" + pair.getValue() / num + "\n");
            /*
            if(i >= 40){
                break;
            }
            i++;
            */
        }
        bw.close();
    }
}
