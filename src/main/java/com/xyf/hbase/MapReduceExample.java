// This file is part of HBaseClientExample.
// Copyright (C) 2015-2016 The Operate-Monitor Authors.
//
package com.xyf.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MapReduceExample extends Configured implements Tool{

private static final Log LOG = LogFactory.getLog(MapReduceExample.class);
private static final String TABLE_NAME = "SOU";

/** Name of this 'program'. */
static final String NAME = "rowcounter";

private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";
private final static String EXPECTED_COUNT_KEY = MapReduceExample.class.getName() + ".expected_count";


/**
 * Mapper that runs the count.
 */
static class RowCounterMapper
        extends TableMapper<Text, IntWritable> {
    private final IntWritable ONE = new IntWritable(1);
    private Text text = new Text();

    @Override
    public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
            throws IOException, InterruptedException {

        String val = new String(columns.getValue(Bytes.toBytes("record"), Bytes.toBytes("userid")));
        text.set(val);
        context.write(text, ONE);
    }
}

static class RowCounterReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
    public void reduce(Text siteid,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            Integer intVal = new Integer(val.toString());
            sum += intVal;
        }
        Put put = new Put(Bytes.toBytes(siteid.toString()));
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("count"), Bytes.toBytes(sum));
        context.write(null,put);
    }
}


    /**
     * Sets up the actual job.
     *
     * @param conf  The current configuration.
     * @param args  The command line parameters.
     * @return The newly created job.
     * @throws IOException When setting up the job fails.
     */
    public static Job createSubmittableJob(Configuration conf, String[] args)
            throws IOException {
        String tableName = TABLE_NAME;
        String targetTable = "res";
        long startTime = 1452441600000L;
        long endTime = 1452528000000L;

        Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
        job.setJarByClass(MapReduceExample.class);
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        String family = "record";
        String qualifier = "userid";
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setTimeRange(startTime, endTime);
        System.out.println(scan.toJSON());
        //job.setOutputFormatClass(NullOutputFormat.class);// because we aren't emitting anything from mapper
        TableMapReduceUtil.initTableMapperJob(
                tableName,  //input table
                scan, // Scan instance to control CF and attribute selection
                RowCounterMapper.class, // mapper class
                Text.class, //mapper output key
                IntWritable.class, //mapper output value
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                targetTable,    //output table
                RowCounterReducer.class,    // reducer class
                job
        );
        job.setNumReduceTasks(1); //at least one
        return job;
    }

    /*
     * @param errorMessage Can attach a message when error occurs.
     */
    private static void printUsage(String errorMessage) {
        System.err.println("ERROR: " + errorMessage);
        printUsage();
    }

    /**
     * Prints usage without error message.
     * Note that we don't document --expected-count, because it's intended for test.
     */
    private static void printUsage() {
        System.err.println("Usage: RowCounter [options] <tablename> " +
                "[--starttime=[start] --endtime=[end] " +
                "[--range=[startKey],[endKey]] [<column1> <column2>...]");
        System.err.println("For performance consider the following options:\n"
                + "-Dhbase.client.scanner.caching=100\n"
                + "-Dmapreduce.map.speculative=false");
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage("Wrong number of parameters: " + args.length);
            return -1;
        }
        Job job = createSubmittableJob(getConf(), args);
        if (job == null) {
            return -1;
        }
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    /**
     * Main entry point.
     * @param args The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        int errCode = ToolRunner.run(HBaseConfiguration.create(), new MapReduceExample(), args);
        System.exit(errCode);
    }

}