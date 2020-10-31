package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.mortbay.io.nio.SelectorManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//calculate the likelihood of each term in each class,that is P(tk|Ci)=the number of tk in class ci/the number of all term in ci
public class TermsNumOfEachClass extends Configured implements Tool {
    //store <classname,all term number> in map,so we can calculate likelihood

    static class TermsNumMapper extends Mapper<Text, BytesWritable,Text, TermNumPair>{
        private static final Pattern PATTERN = Pattern.compile("[/sa-zA-Z]+");
        //TODO:stop list
//        private static String[] stopWordsArray = { "A", "a", "the", "an", "in",
//                "on", "and", "The", "As", "as", "AND" };

        //function map's input format is <classname+filename,byteswritbale>
        //function map's output format is <classname,<term,intwritbale>>
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            Text className=new Text();
            IntWritable one=new IntWritable(1);
            TermNumPair termNumPair=new TermNumPair();

            className.set(key.toString().split("\\+")[0]);
            termNumPair.setNumber(one);

            String contents=new String(value.getBytes());
            Matcher matcher=PATTERN.matcher(contents);
            while (matcher.find()){
                String term = matcher.group();
                termNumPair.setTerm(new Text(term));
                context.write(className,termNumPair);
            }
        }
    }
    static class TermsNumReducer extends Reducer<Text,TermNumPair,Text, TermNumPair>{
        @Override
        protected void reduce(Text key, Iterable<TermNumPair> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            HashMap<String,Integer> termNumMap =new HashMap<>();

            for (TermNumPair pair:
                    values) {
                String term=pair.getTerm().toString();
                if (termNumMap.containsKey(term)){
                    int tempNum = termNumMap.get(term) +pair.getNumber().get();
                    termNumMap.put(term,tempNum);
                }else {
                    termNumMap.put(term,pair.getNumber().get());
                }
            }
            for (HashMap.Entry<String, Integer> entry : termNumMap.entrySet()) {
                context.write(key, new TermNumPair(new Text(entry.getKey()), new IntWritable(entry.getValue())));
            }

        }


    }
    @Override
    public int run(String[] strings) throws Exception {
        //output file should have this directory tree:
        //TermsNumOfEachTerm/--classname-/
        Configuration conf=getConf();

        Path inputpath=new Path(conf.get("INPUTPATH"));
        Path outputpath=new Path(conf.get("OUTPUTPATH"));

        FileSystem fs=outputpath.getFileSystem(conf);
        //delete old output
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
        }

        Job job= Job.getInstance(conf,"TermsNumOfEachClass");
        job.setJarByClass(TermsNumOfEachClass.class);
        job.setMapperClass(TermsNumOfEachClass.TermsNumMapper.class);
        job.setCombinerClass(TermsNumOfEachClass.TermsNumReducer.class);
        job.setReducerClass(TermsNumOfEachClass.TermsNumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TermNumPair.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        //添加文件的输入路径
        SequenceFileInputFormat.addInputPath(job, inputpath);
        //添加文件的输出路径
        FileOutputFormat.setOutputPath(job, outputpath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
