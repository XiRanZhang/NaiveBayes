package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NaiveBayes extends Configured implements Tool {


    //for now.we have  two types of input file,one,<classname,float>,another,<classname@term,float>
    static class NaiveBayesMapper extends Mapper<Text, BytesWritable,Text, Text>{
        private static Map<String, Double> classPriorProbability ; // 类的先验概率
        private static Map<String, Map<String,Integer>> termslikelihood ; // 每个单词在类中的条件概率
        private static final Pattern PATTERN = Pattern.compile("[/sa-zA-Z]+");
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            classPriorProbability =new HashMap<>();
            termslikelihood=new HashMap<>();
            //read the contents of input files into java maps
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            //this path is true only when this is one spilt
            Path priorProbPath = new Path(Utils.DOCs_NUM_OF_EACH_CLASS_OUTPUT_PATH+ "/part-r-00000" );
            Path likelihoodPath = new Path(Utils.TERMs_NUM_OF_EACH_CLASS_OUTPUT_PATH + "/part-r-00000");
            FSDataInputStream inputStream = null;
            BufferedReader bfreader=null;
            try {
                inputStream=fileSystem.open(priorProbPath);
                bfreader = new BufferedReader(new InputStreamReader(inputStream));
                String line=null;
                while (( line = bfreader.readLine()) != null) {
                    String tokens[] = line.split("\t");
                    classPriorProbability.put(tokens[0],Double.parseDouble(tokens[1]));
            }
                inputStream=fileSystem.open(likelihoodPath);
                bfreader = new BufferedReader(new InputStreamReader(inputStream));
                while (( line = bfreader.readLine()) != null) {
                    String tokens[] = line.split("\t");
                    String classname=tokens[0];
                    HashMap<String,Integer> termNumMap;
                    //TODO:put file contents into map
                    if (termslikelihood.containsKey(classname)){
                        termNumMap =(HashMap<String, Integer>) termslikelihood.get(classname);
                        termNumMap.put(tokens[1],Integer.parseInt(tokens[2]));
                        termslikelihood.put(classname,termNumMap);
                    }else{
                        termNumMap =new HashMap<>();
                        termNumMap.put(tokens[1],Integer.parseInt(tokens[2]));
                        termslikelihood.put(classname,termNumMap);
                    }
                }
//                System.out.println(termslikelihood);

            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '");
            }finally {
                bfreader.close();
                inputStream.close();
                fileSystem.close();
            }
        }

        //the function output format should be  <classname@filename, classnameByClassifier>
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            //start to compute prior probability

            String contents=new String(value.getBytes());
            Matcher matcher=PATTERN.matcher(contents);
            while (matcher.find()){
                String term = matcher.group();

            }

        }


    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        //input file includes two files.
        Path inputpath=new Path(conf.get("INPUTPATH"));
        Path outputpath=new Path(conf.get("OUTPUTPATH"));

        FileSystem fs=outputpath.getFileSystem(conf);
        //delete old output
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
        }

        Job job= Job.getInstance(conf,"NaiveBayes");
        job.setJarByClass(NaiveBayes.class);
        job.setMapperClass(NaiveBayes.NaiveBayesMapper.class);
//        job.setCombinerClass(NaiveBayes.TermsNumMapper.TermsNumReducer.class);
//        job.setReducerClass(NaiveBayes.TermsNumMapper.TermsNumReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);


        //添加文件的输入路径
        SequenceFileInputFormat.addInputPath(job, inputpath);
        //添加文件的输出路径
        FileOutputFormat.setOutputPath(job, outputpath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
