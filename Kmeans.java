import java.io.IOException;
import java.util.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.Month;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Kmeans {


 public double EuclideanDistance(double n1, double n2)throws IOException,InterruptedException  {
    double distance = 0;
    distance = n2 - n1;
    return distance;
 }
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static class SortMap extends Mapper<LongWritable, Text, IntWritable, Text> {
   

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
            String token1 = tokenizer.nextToken();
            String token2 = tokenizer.nextToken();
            context.write(new IntWritable(Integer.parseInt(token2)), new Text(token1));
        
    }
 }

 public static class SortReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val);
        }
    }
 }

 public static class KMap extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    //紀錄四個中心點
    private List<String> centroDate = new ArrayList<>();
    private List<Integer> centroValue = new ArrayList<>();
    //int random 1-120
    //int random = (int)(Math.random() * 120 + 1);
    private int localMin = 0;
    private String localIndex = "";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String new_key = tokenizer.nextToken(); //時間

        //初始化四個中心點
        centroDate.add("K1");
        centroDate.add("K2");
        centroDate.add("K3");
        centroDate.add("K4");
        //隨機四個中心點
        centroValue.add(20);
        centroValue.add(40);
        centroValue.add(60);
        centroValue.add(80);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken(); 
            int new_value = Integer.parseInt(token);

            List<Double> list_distances = new ArrayList<>();

            //計算距離
            for(int i=0;i<centroValue.size();i++){
                double distance = Math.sqrt(Math.pow((new_value - centroValue.get(i)),2));
                list_distances.add(distance);
            }
            //找出最近的中心點並分類
            for(int i=0;i<centroValue.size();i++){
                if(Collections.min(list_distances) == Math.sqrt(Math.pow((new_value - centroValue.get(i)),2))){
                    context.write(new Text(centroDate.get(i)), new IntWritable(new_value));
                }
            }
            
        }       
        
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        //context.write(new Text(localIndex), new IntWritable(localMin));
        //context.write(tmp1, tmp2);
    }

 }

 public static class KReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int sum =0;
    private int count =0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        
        for (IntWritable val : values) {
            sum+=val.get();
            count++;
            //context.write(key, val);
        }
        //得到該類別的中心點
        context.write(key, new IntWritable(sum/count));
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        
        //context.write(new Text(localIndex), new IntWritable(localMin));
        //context.write(tmp1, tmp2);
    }

 }


 public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    
    Job job = new Job(conf, "kmeans");  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(KMap.class);
    job.setReducerClass(KReduce.class);
    job.setJarByClass(Kmeans.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);


    /*
    Configuration conf2 = new Configuration();

    Job job2 = new Job(conf, "sort_count");
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setMapperClass(SortMap.class);
    job2.setReducerClass(SortReduce.class);
    job2.setJarByClass(WordCount.class);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    job2.waitForCompletion(true);
    */

    /*
    int repeated = 0;

    do{
        Configuration conf = new Configuration();
    
        Job job = new Job(conf, "kmeans");  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(KMap.class);
        job.setReducerClass(KReduce.class);
        job.setJarByClass(Kmeans.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + repeated));
        
        job.waitForCompletion(true);

        ++repeated;

    }while(repeated < 30);
    
    */
 }
        
}
