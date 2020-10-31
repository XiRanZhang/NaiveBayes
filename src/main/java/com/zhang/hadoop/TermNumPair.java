package com.zhang.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TermNumPair implements WritableComparable<TermNumPair> {
    private Text term;
    private IntWritable number;

    public TermNumPair() {
        this.term=new Text();
        this.number=new IntWritable();
    }

    public TermNumPair(Text term, IntWritable number) {
        this.term = term;
        this.number = number;
    }

    public Text getTerm() {
        return term;
    }

    public void setTerm(Text term) {
        this.term = term;
    }

    public IntWritable getNumber() {
        return number;
    }

    public void setNumber(IntWritable number) {
        this.number = number;
    }

    @Override
    public String toString() {
        return term+"\t"+number;
    }

    @Override
    public int compareTo(TermNumPair termNumPair) {
        if (this.term.compareTo(termNumPair.term) == 0)
            return this.number.compareTo(termNumPair.number);
        else
            return this.term.compareTo(termNumPair.term);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        term.write(dataOutput);
        number.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        term.readFields(dataInput);
        number.readFields(dataInput);
    }
}
