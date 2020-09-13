package hadoop01;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFStest {

		
		Configuration  conf = null;
		FileSystem  fs = null;
		
		
		@Before
		public void conn() throws Exception{
			
			conf = new Configuration(false);
			conf.set("fs.defaultFS", "hdfs://192.168.21.150:9000");
			fs = FileSystem.get(conf);
		}
		
		@After
		public void close() throws Exception{
			fs.close();
		}
		
		@Test
		public void testConf(){
			System.out.println(conf.get("fs.defaultFS"));
			
		}
		
		
		@Test
		public void mkdir() throws Exception{
			
			Path dir = new Path("/sxt");
			if(!fs.exists(dir)){
				fs.mkdirs(dir);
			}
		}
		
		
		@Test
		public void uploadFile() throws Exception{
			
			
			Path file = new Path("/sxt/startup.sh");
			FSDataOutputStream output = fs.create(file );
			
			InputStream input = new BufferedInputStream(new FileInputStream(new File("E:\\startup.sh")) ) ;
			
			IOUtils.copyBytes(input, output, conf, true);
			
		}
		
		@Test
		public void blk() throws Exception{
			
			
			Path file = new Path("/sxt/startup.sh");
			FileStatus ffs = fs.getFileStatus(file );
			BlockLocation[] blks = fs.getFileBlockLocations(ffs , 0, ffs.getLen());
			
			for (BlockLocation b : blks) {
				
				System.out.println(b);
				HdfsBlockLocation hbl = (HdfsBlockLocation)b;
				System.out.println(hbl.getLocatedBlock().getBlock().getBlockId());
			}
			
			FSDataInputStream input = fs.open(file);
			
			System.out.println((char)input.readByte());
			
			input.seek(1048576);
			
			System.out.println((char)input.readByte());
			
		}
		
		
}
		


