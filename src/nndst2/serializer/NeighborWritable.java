package nndst.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class NeighborWritable implements WritableComparable<NeighborWritable>, Cloneable 
{
	private final static BooleanWritable B  =new BooleanWritable(true);
	private final static BooleanWritable RT =new BooleanWritable(false);
//	private final static IntWritable T =new IntWritable(2);
	
	//----------------------------------------------------
	
	protected IntWritable        node;
	protected FloatArrayWritable point;
	
	protected IntWritable        remain;
	protected FloatWritable      distToKey;
	
//	protected IntWritable Flag;
//	protected IntWritable New;
	protected BooleanWritable    symbol;
	
	//====================================================
	
	public NeighborWritable() {
		set(new IntWritable(), new FloatArrayWritable(), new IntWritable(0), new FloatWritable(0));
		
//		this.Flag =new IntWritable(0);
//		this.New  =new IntWritable(0);
		this.symbol =new BooleanWritable(B.get());
	}
	
	//TODO: Review this function: Is this really necessary? 
	public NeighborWritable(int node, int dim, float distToKey) {
		FloatWritable[]    tmpAry1 =new FloatWritable[dim];
		FloatArrayWritable tmpAry2 =new FloatArrayWritable();
		
		for (int i =0; i < dim; i++)
			tmpAry1[i] =new FloatWritable(Float.MAX_VALUE);
		
		tmpAry2.set(tmpAry1);
		
		set(new IntWritable(node), tmpAry2, new IntWritable(0), new FloatWritable(distToKey));
		
		this.symbol =new BooleanWritable(B.get());
//		this.New  =new IntWritable(0);
	}
	
	public NeighborWritable(int node, float[] point, float distToKey) {
		FloatWritable[]    tmpAry1 =new FloatWritable[point.length];
		FloatArrayWritable tmpAry2 =new FloatArrayWritable();
		
		for (int i =0; i < tmpAry1.length; i++) 
			tmpAry1[i] =new FloatWritable(point[i]);
		
		tmpAry2.set(tmpAry1);
		
		set(new IntWritable(node), tmpAry2, new IntWritable(0), new FloatWritable(distToKey));
		
		this.symbol =new BooleanWritable(B.get());
//		this.New  =new IntWritable(0);
	}
	
	public NeighborWritable(IntWritable node, FloatWritable[] point, FloatWritable distToKey) {
		FloatArrayWritable tmpAry =new FloatArrayWritable();
		tmpAry.set(point);
		
		set(node, tmpAry, new IntWritable(0), distToKey);
		
		this.symbol =new BooleanWritable(B.get());
//		this.New  =new IntWritable(0);
	}
	
	public NeighborWritable(IntWritable node, FloatArrayWritable point, FloatWritable distToKey) {
		set(node, point, new IntWritable(0), distToKey);
		
		this.symbol  =new BooleanWritable(B.get());
//		this.New  =new IntWritable(0);
		this.remain =new IntWritable(0);
	}
	
	public void set(IntWritable node, FloatArrayWritable point, IntWritable remain, FloatWritable distToKey) {
		this.node      =node;
		this.point     =point;
		this.remain    =remain;
		this.distToKey =distToKey;
	}
	
	public void setDist(float l) { this.distToKey =new FloatWritable(l); }
	
	//---------------------------------------------------------------------
	
	public IntWritable getWritableNode() { return node; }
	public int         getIntNode()      { return node.get(); }
	
	public FloatArrayWritable getWritablePoint() { return point; }
	public float[] getFloatPoint() {
		FloatWritable[] tmpAry;
		float[]         point;
		
		tmpAry =(FloatWritable[])this.point.toArray();
		point  =new float[tmpAry.length];
		for (int i =0; i <tmpAry.length; i++) 
			point[i] =tmpAry[i].get();
		
		return point;
	}
	
	public IntWritable     getWritableRemain() { return remain; }
	public int             getIntRemain()      { return remain.get(); }
	
	public FloatWritable  getWritableDist() { return distToKey; }
	public float          getFloatDist()   { return distToKey.get(); }
	
	public BooleanWritable getWritableSymbol() { return symbol; }
	public boolean         getBooleanSymbol()  { return symbol.get(); }
	
//	public IntWritable    getWritableNew()  { return New; }
//	public int            getIntNew()       { return New.get(); }
	
	//---------------------------------------------------------------------
	
//	public void markFlagAsTrue()    { this.remain =TRUE; }
//	public void markFlagAsNeutral() { this.remain =NEUTRAL; }
//	public void markFlagAsFalse()   { this.remain =FALSE; }
	
//	public void markNewAsTrue()     { this.New =TRUE; }
//	public void markNewAsNeutral()  { this.New =NEUTRAL; }
//	public void markNewAsFalse()    { this.New =FALSE; }
	
	public void markSymbolAsTrue()    { this.symbol =B; }
//	public void markNewRAsNeutral() { this.remain =NEUTRAL; }
	public void markSymbolAsFalse()   { this.symbol =RT; }
	
	public void incrementRemain() {
		int r =this.remain.get();
		
		this.remain.set(r + 1);
	}
	
	public void setRemain(int c) {
		this.remain =new IntWritable(c);
	}
	
	//---------------------------------------------------------------------
	
//	public boolean isFlagTrue()    { return this.Flag.get() == TRUE.get(); }
//	public boolean isFlagNeutral() { return this.Flag.get() == NEUTRAL.get(); }
//	public boolean isFlagFalse()   { return this.Flag.get() == FALSE.get(); }
	
//	public boolean isNewTrue()    { return this.New.get() == TRUE.get(); }
//	public boolean isNewNeutral() { return this.New.get() == NEUTRAL.get(); }
//	public boolean isNewFalse()   { return this.New.get() == FALSE.get(); }
	
//	public boolean isSymbolTrue()    { return this.symbol.get(); }
//	public boolean isNewRNeutral() { return this.NewR.get() == NEUTRAL.get(); }
//	public boolean isSymbolFalse()   { return !this.symbol.get(); }
	
	//---------------------------------------------------------------------
	
	@Override
	public void write(DataOutput out) throws IOException {
		node.write(out);
		point.write(out);
		
		remain.write(out);
		distToKey.write(out);
		
		symbol.write(out);
//		NewR.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		node.readFields(in);
		point.readFields(in);
		
		remain.readFields(in);
		distToKey.readFields(in);
		
		symbol.readFields(in);
//		NewR.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return node.hashCode()*163;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof NeighborWritable) {
			NeighborWritable nw =(NeighborWritable) o;
			return node.equals(nw.node);
		}
		return false;
	}
	
	@Override
	public String toString() {
		String s =symbol.get() ? "B" : "RT";
		return "node:" + node.toString() + " remain = " + remain + " dist = " + distToKey.toString() + " symbol = " + s;
	}
	
	@Override
	public int compareTo(NeighborWritable niw) {
		return distToKey.compareTo(niw.distToKey);
	}
	
	@Override
	public NeighborWritable clone() {
		int     node      =this.getIntNode();
		float[] point     =this.getFloatPoint();
		float   distToKey =this.getFloatDist();
		
		NeighborWritable cln =new NeighborWritable(node, point, distToKey);
		
		cln.setRemain(this.getIntRemain());
		if      (this.getBooleanSymbol())  cln.markSymbolAsTrue();
		else                               cln.markSymbolAsFalse();
		
//		if      (this.getIntNew() == TRUE.get())  cln.markNewAsTrue();
//		else if (this.getIntNew() == FALSE.get()) cln.markNewAsFalse();
//		
//		if      (this.getIntNewR() == TRUE.get())  cln.markNewRAsTrue();
//		else if (this.getIntNewR() == FALSE.get()) cln.markNewRAsFalse();
 		
		return cln;
		// consider remain's atsukai
	}
}
