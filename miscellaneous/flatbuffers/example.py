import Tables.FB_Meta as mt
import flatbuffers
builder = flatbuffers.Builder(0)

mt.FB_MetaStartBlobDataVector(builder,2)
builder.PrependByte(1)
builder.PrependByte(2)
datablob = builder.EndVector(2)


mt.FB_MetaStart(builder)
mt.FB_MetaAddBlobFormat(builder,1)
mt.FB_MetaAddBlobData(builder,datablob)
mt.FB_MetaAddBlobSize(builder, 2)
mt.FB_MetaAddBlobDeleted(builder, False)
mt.FB_MetaAddBlobDeleted(builder, False)
mt.FB_MetaAddBlobOrigOff(builder, 0)
mt.FB_MetaAddBlobOrigLen(builder, 2)
mt.FB_MetaAddBlobCompression(builder, 0)
ocr = mt.FB_MetaEnd(builder)
builder.Finish(ocr)
final_flatbuffer = builder.Output()

obj = mt.FB_Meta()
obj = obj.GetRootAsFB_Meta(final_flatbuffer,0)

obj.BlobDataLength()
obj.BlobFormat()

#SSL
#flatbuffer
#jupyter examplee