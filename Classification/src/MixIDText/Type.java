package MixIDText;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Type implements org.apache.hadoop.io.Writable {
	public DoubleWritable doubleWritableObj = null;
	public Text textObj = null;

	public Type() {
		 doubleWritableObj = new DoubleWritable(0);
		 textObj = new Text("notsetyet!");
	}

	public Type(DoubleWritable value) {
		super();
		doubleWritableObj = value;

	}

	public Type(Text key) {
		super();
		textObj = key;
	}

	public DoubleWritable getDoubleWritableObj() {
		return doubleWritableObj;
	}

	public void setDoubleWritableObj(DoubleWritable longWritableObj) {
		this.doubleWritableObj = longWritableObj;
	}

	public Text getTextObj() {
		return textObj;
	}

	public void setTextObj(Text textObj) {
		this.textObj = textObj;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		if (doubleWritableObj != null) {
			doubleWritableObj = new DoubleWritable(in.readDouble());
			// doubleWritableObj.readFields(in);
		}
		if (textObj != null) {
			textObj = new Text(in.readUTF());

			// textObj.readFields(in);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		if (doubleWritableObj != null) {
			out.writeDouble(this.doubleWritableObj.get());

		}
		if (textObj != null) {
			out.writeUTF(this.textObj.toString());

		}
	}
	/*
	 * public static Type read(DataInput in) throws IOException { Type w = new
	 * Type(); w.readFields(in); return w; }
	 */
	/*
	 * @Override public int compareTo(Type typeObj) { int cmp =
	 * typeObj.getLongWritableObj().compareTo(doubleWritableObj); if (cmp != 0) {
	 * return cmp; } return typeObj.getTextObj().compareTo(textObj); }
	 */

}
