package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Evaluation {


    public Evaluation() throws IOException {
        //java 单机程序 评估分类效果
        //read the contents of input files into java maps
        Configuration configuration = new Configuration();

        //this path is true only when this is one spilt
        Path resultOfClassficationPath = new Path(Utils.RESULT_OF_CLASSFICATION+ "/part-r-00000" );
        FSDataInputStream inputStream = null;
        BufferedReader bfreader=null;
        FileSystem fileSystem = FileSystem.get(configuration);
        try{
            inputStream=fileSystem.open(resultOfClassficationPath);
            bfreader = new BufferedReader(new InputStreamReader(inputStream));
            String line=null;
            //read <classname,docNum> into hash map classPriorProbability
            while (( line = bfreader.readLine()) != null) {
                String[] tokens = line.split("\t");
                String realClass=tokens[0].split("\\+")[0];
                String classifier =tokens[1];
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if (bfreader != null)
                bfreader.close();
            if (inputStream !=null)
                inputStream.close();
            if (fileSystem !=null)
                fileSystem.close();
        }
        }
}
