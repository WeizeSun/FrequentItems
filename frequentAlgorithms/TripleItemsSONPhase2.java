package frequentAlgorithms;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
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

public class TripleItemsSONPhase2{
    private static int CHUNKSIZE = 200000;
    private static double s = 0.0025;
    private static double num = 945127;
    public static class NewInputFormat extends TextInputFormat{
        @Override
        public RecordReader<LongWritable, Text>createRecordReader(InputSplit split, TaskAttemptContext context){
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
            Configuration conf = context.getConfiguration();
            String[] candidatesString = conf.get("candidates").split("\\s+");
            Set<String> candidates = new HashSet<String>();
            for(String candidate: candidatesString){
                candidates.add(candidate);
            }
            String lines = value.toString();
            String[] lineArr = lines.split("\n");
            int lcount = lineArr.length;
            for(String line: lineArr){
                String[] words = line.split("\\s+");
                for(String wordOne: words){
                    for(String wordTwo: words){
                        for(String wordThree: words){
                            if(wordOne.compareTo(wordTwo) < 0 && wordOne.compareTo(wordThree) < 0 && wordTwo.compareTo(wordThree) < 0 && candidates.contains(wordOne+","+wordTwo+","+wordThree)){
                                context.write(new Text(wordOne+","+wordTwo+","+wordThree), new IntWritable(1));
                            }
                        }
                    }
                }
            }
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            if(sum / num >= s){
                context.write(key, new IntWritable(sum));
            }
        }
    }
    public static void main(String[] args) throws Exception{
        BufferedReader bf = new BufferedReader(new FileReader("candidates"));
        String candidates = "";
        String line;
        while((line = bf.readLine()) != null){
            String[] parts = line.split("\\s+");
            candidates += parts[0];
            candidates += " ";
        }
        Configuration conf = new Configuration();
        conf.set("candidates", candidates);
        Job job = Job.getInstance(conf, "SONPhaseTwo");
        job.setJarByClass(TripleItemsSONPhase2.class);
        job.setInputFormatClass(NewInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
