package nndst.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class NodeWritable implements WritableComparable<NodeWritable>, Cloneable {
	
	protected IntWritable        nodeId;
	protected FloatArrayWritable point;
	protected BooleanWritable    sampled;
	
	//=================================================================
	
	public NodeWritable() {
		FloatWritable[]    nullAry1 =new FloatWritable[0];
		FloatArrayWritable nullAry2 =new FloatArrayWritable();
		
		nullAry2.set(nullAry1);
		
		set(new IntWritable(0), nullAry2);
	}
	
	public NodeWritable(int id) {
		FloatWritable[]    nullAry1 =new FloatWritable[0];
		FloatArrayWritable nullAry2 =new FloatArrayWritable();
		
		nullAry2.set(nullAry1);
		
		set(new IntWritable(id), nullAry2);
	}
	
	public NodeWritable(int id, float[] point) {
		FloatWritable[]    tmpAry1 =new FloatWritable[point.length];
		FloatArrayWritable tmpAry2 =new FloatArrayWritable();
		
		for (int i =0; i < tmpAry1.length; i++) 
			tmpAry1[i] =new FloatWritable(point[i]);
		
		tmpAry2.set(tmpAry1);
		
		set(new IntWritable(id), tmpAry2);
	}
	
	public NodeWritable(IntWritable id, FloatArrayWritable point) {
		set(id, point);
	}
	
	public void set(IntWritable id, FloatArrayWritable point) {
		this.nodeId  =id;
		this.point   =point;
		this.sampled =new BooleanWritable(false);
	}
	
	//------------------------------------------------------------------
	
	public IntWritable getWritableNode() { return nodeId; }
	public int         getIntNode()      { return nodeId.get(); }
	
	public FloatArrayWritable getWritablePoint() {	return point; }
	public float[]            getFloatPoint() {
		FloatWritable[] tmpAry;
		float[]         point;
		
		tmpAry =(FloatWritable[])this.point.toArray();
		point  =new float[tmpAry.length];
		for (int i =0; i <tmpAry.length; i++) 
			point[i] =tmpAry[i].get();
		
		return point;
	}
	
	//------------------------------------------------------------------
	
	public void markFlagAsTrue()  { this.sampled =new BooleanWritable(true); }
	public void markFlagAsFalse() { this.sampled =new BooleanWritable(false); }
	
	public boolean isFlagTrue()  { return this.sampled.get(); }
	public boolean isFlagFalse() { return !this.sampled.get(); }
	
	//------------------------------------------------------------------
	
	public NodeWritable deepCopy() {
		int     nodeId =this.getIntNode();
		float[] point  =this.getFloatPoint();
		
		return new NodeWritable(nodeId, point);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		point.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		point.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return nodeId.hashCode()*163;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof NodeWritable) {
			NodeWritable pw =(NodeWritable) o;
			return nodeId.equals(pw.nodeId);
		} else if (o instanceof Integer) {
			Integer pw =(Integer) o;
			return nodeId.get() == pw.intValue();
		}
		return false;
	}
	
	@Override
	public String toString() {
		FloatWritable[] tmpAry =(FloatWritable[])this.point.toArray();
		String          line   =nodeId.toString() + ": ";
		
		for (int i =0; i <tmpAry.length; i++)
			line +=tmpAry[i].toString() + " ";
		
		return line;
	}
	
	@Override
	public int compareTo(NodeWritable pw) {
		return nodeId.compareTo(pw.nodeId);
	}
	
	@Override
	public NodeWritable clone() {
		try                                  { return (NodeWritable) super.clone(); }
		catch (CloneNotSupportedException e) { throw new InternalError(e.toString()); }
	}
}
