package frequentAlgorithms;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class DoubleItemsSONPhase1{
    private static int CHUNKSIZE = 200000;
    private static double s = 0.005;
    private static double num = 945127;
    public static class NewInputFormat extends TextInputFormat{
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
            return new NewRecordReader();
        }
    }
    public static class NewRecordReader extends RecordReader<LongWritable, Text>{
        private LineReader in;
        private LongWritable key;
        private Text value = new Text();
        private long start = 0;
        private long end = 0;
        private long pos = 0;
        private int maxLineLength;

        @Override
        public void close() throws IOException{
            if(in != null){
                in.close();
            }
        }
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException{
            return key;
        }
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException{
            return value;
        }
        @Override
        public float getProgress() throws IOException, InterruptedException{
            if(start == end){
                return 0.0f;
            }
            else{
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException{
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end = start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());

            if(start != 0){
                skipFirstLine = true;
                start--;
                filein.seek(start);
            }
            in = new LineReader(filein, conf);
            if(skipFirstLine){
                start += in.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException{
            if(key == null){
                key = new LongWritable();
            }
            key.set(pos);
            if(value == null){
                value = new Text();
            }
            value.clear();
            final Text endline = new Text("\n");
            int newSize = 0;
            for(int i = 0; i < CHUNKSIZE; i++){
                Text v = new Text();
                while(pos < end){
                    newSize = in.readLine(v, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                    value.append(v.getBytes(), 0, v.getLength());
                    value.append(endline.getBytes(), 0, endline.getLength());
                    if(newSize == 0){
                        break;
                    }
                    pos += newSize;
                    if(newSize < maxLineLength){
                        break;
                    }
                }
            }
            if(newSize == 0){
                key = null;
                value = null;
                return false;
            }
            else{
                return true;
            }
        }
    }   
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String lines = value.toString();
            String[] lineArr = lines.split("\n");
            int lcount = lineArr.length;
            double chunks = s * lcount / num;
            Hashtable<String, Integer>singletons = new Hashtable<String, Integer>();
            Hashtable<String, Integer>doubletons = new Hashtable<String, Integer>();
            for(String line: lineArr){
                StringTokenizer words = new StringTokenizer(line);
                while(words.hasMoreTokens()){
                    String word = words.nextToken();
                    if(singletons.containsKey(word)){
                        singletons.put(word, singletons.get(word) + 1);
                    }
                    else{
                        singletons.put(word, 1);
                    }
                }
            }
            for(Iterator<Entry<String, Integer>>it = singletons.entrySet().iterator(); it.hasNext(); ){
                Entry<String, Integer>temp = it.next();
                if((double)temp.getValue() / lcount < s){
                    it.remove();
                }
            }
            for(String line: lineArr){
                StringTokenizer words = new StringTokenizer(line);
                List<String> candidates = new ArrayList<String>();
                while(words.hasMoreTokens()){
                    String word = words.nextToken();
                    if(singletons.containsKey(word)){
                        candidates.add(word);
                    }
                }
                for(String keyOne: candidates){
                    for(String keyTwo: candidates){
                        if(keyOne.compareTo(keyTwo) < 0){
                            if(doubletons.containsKey(keyOne+","+keyTwo)){
                                doubletons.put(keyOne+","+keyTwo, doubletons.get(keyOne+","+keyTwo) + 1);
                            }
                            else{
                                doubletons.put(keyOne+","+keyTwo, 1);
                            }
                        }
                    }
                }
            }
            for(Iterator<Entry<String, Integer>>it = doubletons.entrySet().iterator(); it.hasNext(); ){
                Entry<String, Integer>temp = it.next();
                if((double)temp.getValue() / lcount < s){
                    it.remove();
                }
            }
            ArrayList<Entry<String, Integer>>sorted_doubletons = new ArrayList(doubletons.entrySet());
            Collections.sort(sorted_doubletons, new Comparator<Entry<String, Integer>>(){
                public int compare(Entry<String, Integer>pairOne, Entry<String, Integer>pairTwo){
                    return pairTwo.getValue().compareTo(pairTwo.getValue());
                }
            });
            for(Entry<String, Integer>pair: sorted_doubletons){
                context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
            }
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SONPhaseOne");
        job.setJarByClass(DoubleItemsSONPhase1.class);
        job.setInputFormatClass(NewInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
