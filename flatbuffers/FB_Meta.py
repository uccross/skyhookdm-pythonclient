# automatically generated by the FlatBuffers compiler, do not modify

# namespace: Tables

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class FB_Meta(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsFB_Meta(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = FB_Meta()
        x.Init(buf, n + offset)
        return x

    # FB_Meta
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # FB_Meta
    def BlobFormat(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

    # FB_Meta
    def BlobData(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # FB_Meta
    def BlobDataAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Uint8Flags, o)
        return 0

    # FB_Meta
    def BlobDataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FB_Meta
    def BlobDataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

    # FB_Meta
    def BlobSize(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint64Flags, o + self._tab.Pos)
        return 0

    # FB_Meta
    def BlobDeleted(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return bool(self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos))
        return False

    # FB_Meta
    def BlobOrigOff(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint64Flags, o + self._tab.Pos)
        return 0

    # FB_Meta
    def BlobOrigLen(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint64Flags, o + self._tab.Pos)
        return 0

    # FB_Meta
    def BlobCompression(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

def FB_MetaStart(builder): builder.StartObject(7)
def FB_MetaAddBlobFormat(builder, blobFormat): builder.PrependInt32Slot(0, blobFormat, 0)
def FB_MetaAddBlobData(builder, blobData): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(blobData), 0)
def FB_MetaStartBlobDataVector(builder, numElems): return builder.StartVector(1, numElems, 1)
def FB_MetaAddBlobSize(builder, blobSize): builder.PrependUint64Slot(2, blobSize, 0)
def FB_MetaAddBlobDeleted(builder, blobDeleted): builder.PrependBoolSlot(3, blobDeleted, 0)
def FB_MetaAddBlobOrigOff(builder, blobOrigOff): builder.PrependUint64Slot(4, blobOrigOff, 0)
def FB_MetaAddBlobOrigLen(builder, blobOrigLen): builder.PrependUint64Slot(5, blobOrigLen, 0)
def FB_MetaAddBlobCompression(builder, blobCompression): builder.PrependInt32Slot(6, blobCompression, 0)
def FB_MetaEnd(builder): return builder.EndObject()