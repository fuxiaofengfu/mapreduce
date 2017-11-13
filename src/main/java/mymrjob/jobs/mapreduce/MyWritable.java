package mymrjob.jobs.mapreduce;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义map输入输出数据类型
 */
public class MyWritable extends BinaryComparable implements WritableComparable<BinaryComparable>,Serializable {

	private long sum ;
	private String value = "";
	private Map<Object,Object> data = new HashMap<>();

	public MyWritable() {
	}

	public MyWritable(long sum) {
		this.sum = sum;
	}

	public MyWritable(String value) {
		this.value = value;
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//System.out.println("mywritable write .......");
		ByteArrayOutputStream byteArrayOutputStream = null;
		ObjectOutputStream objectOutputStream = null;
		try{
			out.writeLong(sum);
			out.writeUTF(value);
			byteArrayOutputStream = new ByteArrayOutputStream();
			objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(data);
			byte[] bytes = byteArrayOutputStream.toByteArray();
			WritableUtils.writeVInt(out, bytes.length);
			out.write(bytes, 0, bytes.length);
		}finally {
			if(null != byteArrayOutputStream){
				byteArrayOutputStream.close();
			}
			if(null != objectOutputStream){
				objectOutputStream.close();
			}
		}
	}

	/**
	 * Deserialize the fields of this object from <code>in</code>.
	 * <p>
	 * <p>For efficiency, implementations should attempt to re-use storage in the
	 * existing object where possible.</p>
	 *
	 * @param in <code>DataInput</code> to deseriablize this object from.
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		//System.out.println("mywritable readFields .......");

		sum = in.readLong();
		value = in.readUTF();
		int len = WritableUtils.readVInt(in);
		byte[] bytes = new byte[len];
		in.readFully(bytes,0,len);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			data = (HashMap<Object, Object>) objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public long getSum() {
		return sum;
	}

	/**
	 * Return n st bytes 0..n-1 from {#getBytes()} are valid.
	 */
	@Override
	public int getLength() {
		return getBytes().length;
	}
	/**
	 * Return representative byte array for this instance.
	 */
	@Override
	public byte[] getBytes() {

		ByteArrayOutputStream byteArrayOutputStream = null;
		ObjectOutputStream objectOutputStream = null;
		try {
			byteArrayOutputStream = new ByteArrayOutputStream();
			objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(this);
			return byteArrayOutputStream.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				if(null != byteArrayOutputStream){
					byteArrayOutputStream.close();
				}
				if(null != objectOutputStream){
					objectOutputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new byte[0];
	}

	public void setSum(long sum) {
		this.sum = sum;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Map<Object, Object> getData() {
		return data;
	}

	public void setData(Map<Object, Object> data) {
		this.data = data;
	}

	public void setKV(Object key,Object value){
		if(null == this.data){
			this.data = new HashMap<>();
		}
		this.data.put(key,value);
	}

	@Override
	public String toString() {
		return "MyWritable{" +
				"sum=" + sum +
				", value='" + value + '\'' +
				", data=" + data.toString() +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;

		MyWritable that = (MyWritable) o;

		return value.equals(that.value);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = result + value.hashCode();
		return result;
	}

	static {
		WritableComparator.define(MyWritable.class, new Comparator());
	}

	/** A WritableComparator optimized for Text keys. */
	private static class Comparator extends WritableComparator {
		public Comparator() {
			super(MyWritable.class,true);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1,
		                   byte[] b2, int s2, int l2) {

			System.out.println("myWritable comparator .........");

			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
		}
	}
}
