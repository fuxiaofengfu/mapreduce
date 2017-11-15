package mymrjob.jobs.mapreduce;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 公共配置
 */
public class MyJobConf {

    private String jobname;
    private Class<?> jarByClass;
    private Class<? extends Mapper> mapper;

	private Class<? extends Partitioner> partitioner;
	private Class<? extends WritableComparator> comparator;
	private Class<? extends Reducer> combiner;
    private Class<? extends RawComparator> groupComparator;
    private Class<? extends Reducer> reducer;
    private Class<? extends Writable> mapOutKey;
    private Class<? extends Writable> mapOutValue;
    private Class<? extends Writable> reducerOutKey;
    private Class<? extends Writable> reducerOutValue;

	public MyJobConf(String jobname, Class<?> jarByClass) {
		this.jobname = jobname;
		this.jarByClass = jarByClass;
	}

	public MyJobConf(String jobname, Class<?> jarByClass, Class<? extends Reducer> reducer, Class<? extends Mapper> mapper) {
		this.jobname = jobname;
		this.reducer = reducer;
		this.mapper = mapper;
		this.mapOutKey = MyWritable.class;
		this.mapOutValue = MyWritable.class;
		this.reducerOutKey = MyWritable.class;
		this.reducerOutValue = MyWritable.class;
		this.jarByClass = jarByClass;
		this.combiner = AbstractMRJob.MapCombiner.class;
		this.partitioner = AbstractMRJob.MapPartitioner.class;
		this.comparator = AbstractMRJob.MapReduceCompare.class;
		this.groupComparator = AbstractMRJob.GroupComparator.class;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	public Class<? extends Reducer> getReducer() {
		return reducer;
	}

	public void setReducer(Class<? extends Reducer> reducer) {
		this.reducer = reducer;
	}

	public Class<? extends Mapper> getMapper() {
		return mapper;
	}

	public void setMapper(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	public Class<? extends Writable> getMapOutKey() {
		return mapOutKey;
	}

	public void setMapOutKey(Class<? extends Writable> mapOutKey) {
		this.mapOutKey = mapOutKey;
	}

	public Class<? extends Writable> getMapOutValue() {
		return mapOutValue;
	}

	public void setMapOutValue(Class<? extends Writable> mapOutValue) {
		this.mapOutValue = mapOutValue;
	}

	public Class<? extends Writable> getReducerOutKey() {
		return reducerOutKey;
	}

	public void setReducerOutKey(Class<? extends Writable> reducerOutKey) {
		this.reducerOutKey = reducerOutKey;
	}

	public Class<? extends Writable> getReducerOutValue() {
		return reducerOutValue;
	}

	public void setReducerOutValue(Class<? extends Writable> reducerOutValue) {
		this.reducerOutValue = reducerOutValue;
	}

	public Class<?> getJarByClass() {
		return jarByClass;
	}

	public void setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
	}

	public Class<? extends Reducer> getCombiner() {
		return combiner;
	}

	public void setCombiner(Class<? extends Reducer> combiner) {
		this.combiner = combiner;
	}

	public Class<? extends Partitioner> getPartitioner() {
		return partitioner;
	}

	public void setPartitioner(Class<? extends Partitioner> partitioner) {
		this.partitioner = partitioner;
	}

	public Class<? extends WritableComparator> getComparator() {
		return comparator;
	}

	public void setComparator(Class<? extends WritableComparator> comparator) {
		this.comparator = comparator;
	}

	public Class<? extends RawComparator> getGroupComparator() {
		return groupComparator;
	}

	public void setGroupComparator(Class<? extends RawComparator> groupComparator) {
		this.groupComparator = groupComparator;
	}
}
