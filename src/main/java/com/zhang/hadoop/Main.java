package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
//        //converter small file in training data set in sequence file
//        conf.set("INPUTPATH",Utils.BASE_TRAINDATA_PATH);
//        conf.set("OUTPUTPATH",Utils.SEQUENCE_TRAIN_DATA);
//        SmallFilesToSequenceFileConverter sequenceFileConverter = new SmallFilesToSequenceFileConverter();
//        ToolRunner.run(conf, sequenceFileConverter, args);
//
//        //calculate the number of files in each class
//        conf.set("INPUTPATH",Utils.SEQUENCE_TRAIN_DATA);
//        conf.set("OUTPUTPATH",Utils.DOCs_NUM_OF_EACH_CLASS_OUTPUT_PATH);
//        DocsNumOfEachClass docsNumOfEachClass =new DocsNumOfEachClass();
//        ToolRunner.run(conf,docsNumOfEachClass,args);
//
//        //calculate the number of each term in each class
//        conf.set("INPUTPATH",Utils.SEQUENCE_TRAIN_DATA);
//        conf.set("OUTPUTPATH",Utils.TERMs_NUM_OF_EACH_CLASS_OUTPUT_PATH);
//        TermsNumOfEachClass termsNumOfEachClass=new TermsNumOfEachClass();
//        ToolRunner.run(conf,termsNumOfEachClass,args);

        //converter small files in test data in sequence file

        //calculate bayes function
        conf.set("INPUTPATH",Utils.SEQUENCE_TRAIN_DATA);
        conf.set("OUTPUTPATH",Utils.RESULT_OF_CLASSFICATION);
        NaiveBayes naiveBayes =new NaiveBayes();
        ToolRunner.run(conf,naiveBayes,args);
    }


}
