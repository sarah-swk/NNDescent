package nndst.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CommonLib {
	
	public static void delete(File f) {
		if (!f.exists()) return;
		
		if (f.isFile()) f.delete();
		else if (f.isDirectory()) {
			File[] files =f.listFiles();
			
			for (int i =0; i < files.length; i++) {
				delete(files[i]);
			}
			
			f.delete();
		}
		
		return;
	}

	public static void delete(Configuration conf, Path out) throws IOException {
		FileSystem fs =FileSystem.get(conf);
		
		if (fs.exists(out)) fs.delete(out, true);
		
		return;
	}
	
	public static <T> boolean contain(T[] ary, int key) {
		boolean res =false;
		
		for (int i =0; i < ary.length; i++) {
			if (ary[i] == null) break;
			
			if (ary[i].equals(key))
				{ res =true; break; }
		}
		
		return res;
	}
	
	public static boolean contain(int[] ary, int key) {
		boolean res =false;
		
		for (int i =0; i < ary.length; i++) {
//			if (ary[i] == null) break;
			
			if (ary[i] == key)
				{ res =true; break; }
		}
		
		return res;
	}
	
}
