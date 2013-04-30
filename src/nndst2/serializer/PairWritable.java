package nndst.serializer;

public class PairWritable {
	private NodeWritable key;
	private NeighborWritable value;
	
	public PairWritable (NodeWritable key, NeighborWritable value) {
		this.key   =key.deepCopy();
		this.value =value.clone();
	}
	
	public NodeWritable getKey()       { return key; }
	public NeighborWritable getValue() { return value; };
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof PairWritable) {
			PairWritable pw =(PairWritable) o;
			return this.key.equals(pw.key) && this.value.equals(pw.value);
		}
		return false;
	}
	
	@Override
	public PairWritable clone() {
		NodeWritable     key   =this.getKey();
		NeighborWritable value =this.getValue();
		
		return new PairWritable(key, value);
	}
}
