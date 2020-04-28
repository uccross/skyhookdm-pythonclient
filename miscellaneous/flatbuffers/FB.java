import Tables.*;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.*;
import com.google.flatbuffers.*;
public class FB {

	public FB(){
	}

	public static void main(String[] args){
		
		FB_Meta fb = new FB_Meta();	
		FlatBufferBuilder builder = new FlatBufferBuilder();
		String data = "qwfawfiojfewofpfw";
                byte[] buf = data.getBytes();
		int dataoff = fb.createBlobDataVector(builder, buf);
		
		fb.startFB_Meta(builder);
		fb.addBlobFormat(builder, 1);
		fb.addBlobData(builder, dataoff);
			
		int endoff = fb.endFB_Meta(builder);
		fb.finishFB_MetaBuffer(builder,endoff);
		//System.out.println(builder.dataBuffer().duplicate());
		//byte[] b = new byte[builder.dataBuffer().remaining()];
		//builder.dataBuffer().get(b);
		//System.out.println(b);
		System.out.println(endoff);
		FB_Meta fc = FB_Meta.getRootAsFB_Meta(builder.dataBuffer());
		System.out.println(fc.blobFormat());
	}




}

