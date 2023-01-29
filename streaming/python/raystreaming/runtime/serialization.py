from abc import ABC, abstractmethod
import pickle
import pyfury
from raystreaming import message

CROSS_LANG_TYPE_ID = 0
JAVA_TYPE_ID = 1
PYTHON_TYPE_ID = 2


class Serializer(ABC):
    @abstractmethod
    def serialize(self, obj):
        pass

    @abstractmethod
    def deserialize(self, serialized_bytes):
        pass


class PythonSerializer(Serializer):
    def serialize(self, obj):
        return pickle.dumps(obj)

    def deserialize(self, serialized_bytes):
        return pickle.loads(serialized_bytes)


VALUE_MSG_TYPE_ID = -1
RECORD_TYPE_ID = 0
KEY_RECORD_TYPE_ID = 1
VALUE_ARROW_TYPE_ID = 2


class ObjectRowSerializer(pyfury.serializer.Serializer):
    def get_cross_language_type_id(self):
        return pyfury.type.FuryType.FURY_TYPE_TAG.value

    def get_cross_language_type_tag(self):
        return message.ObjectRow.__name__

    def write(self, buffer, value):
        self.cross_language_write(buffer, value)

    def read(self, buffer, type_):
        return self.cross_language_read(buffer, type_)

    # write sequence : rowSize, props, pos, primaryKeys, row fields list
    # need to be consistent with python ObjectRowSerializer
    # in CrossLangSerializer.java
    def cross_language_write(self, buffer, row):
        buffer.write_int32(row.get_arity())
        self.fury_.cross_language_serialize_referencable(
            buffer, row.get_props())
        buffer.write_int32(row.get_pos())
        self.fury_.cross_language_serialize_referencable(
            buffer, row.get_primary_keys())
        object_list = []
        for i in range(row.get_arity()):
            object_list.append(row.get_field(i))
        self.fury_.cross_language_serialize_referencable(buffer, object_list)

    # read sequence : rowSize, props, pos, primaryKeys, row fields list
    # need to be consistent with python ObjectRowSerializer
    # in CrossLangSerializer.java
    def cross_language_read(self, buffer, type_):
        row_size = buffer.read_int32()
        row = message.ObjectRow(row_size)

        row.set_props(
            self.fury_.cross_language_deserialize_referencable(buffer))
        row.set_pos(buffer.read_int32())
        row.set_primary_keys(
            self.fury_.cross_language_deserialize_referencable(buffer))
        field_list = self.fury_.cross_language_deserialize_referencable(buffer)
        for i in range(len(field_list)):
            row.set_field(i, field_list[i])
        return row


class CrossLangSerializer(Serializer):
    """Serialize stream element between java/python"""

    def __init__(self):
        def default_encode(o, buffer):
            binary = self.fury_.serialize(o)
            buffer.write_int32(len(binary))
            buffer.write_binary(binary)

        def default_decode(buffer):
            length = buffer.read_int32()
            binary = buffer.read_binary(length)
            return self.fury_.deserialize(binary)

        def encode_record(o, buffer):
            self.encode(o.stream, buffer)
            self.encode(o.value, buffer)

        def decode_record(buffer):
            stream = self.decode(buffer)
            value = self.decode(buffer)
            record = message.Record(value)
            record.stream = stream
            return record

        def encode_key_record(o, buffer):
            self.encode(o.stream, buffer)
            self.encode(o.key, buffer)
            self.encode(o.value, buffer)

        def decode_key_record(buffer):
            stream = self.decode(buffer)
            key = self.decode(buffer)
            value = self.decode(buffer)
            record = message.KeyRecord(key, value)
            record.stream = stream
            return record

        import pyarrow as pa

        def encode_arrow_batch(batch, buffer):
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            binary = sink.getvalue()
            buffer.write_int32(len(binary))
            buffer.write_binary(binary)

        def decode_arrow_batch(buffer):
            """Zero-copy decode arrow record batch"""
            length = buffer.read_int32()
            buffer_view = pa.py_buffer(buffer).slice(buffer.reader_index)
            # If the input source supports zero-copy reads (e.g. like a memory
            # map, or pa.BufferReader), then the returned batches are also
            # zero-copy and do not allocate any new memory on read.
            # So here the read is zero copy.
            reader = pa.ipc.open_stream(buffer_view)
            record_batch = reader.read_next_batch()
            buffer.reader_index += length
            return record_batch

        self.type_map = {
            message.Record: RECORD_TYPE_ID,
            message.KeyRecord: KEY_RECORD_TYPE_ID,
            pa.RecordBatch: VALUE_ARROW_TYPE_ID
        }
        self.codec_map = {
            RECORD_TYPE_ID: (encode_record, decode_record),
            KEY_RECORD_TYPE_ID: (encode_key_record, decode_key_record),
            VALUE_ARROW_TYPE_ID: (encode_arrow_batch, decode_arrow_batch),
        }
        self.fury_ = pyfury.Fury(language=pyfury.Language.X_LANG)
        self.fury_.register_serializer(
            message.ObjectRow,
            ObjectRowSerializer(self.fury_, message.ObjectRow))
        self.default_encode = default_encode
        self.default_decode = default_decode

    def encode(self, obj, buffer):
        if obj is None:
            type_id = VALUE_MSG_TYPE_ID
        else:
            type_id = self.type_map.get(type(obj), VALUE_MSG_TYPE_ID)
        buffer.write_int8(type_id)
        self.codec_map\
            .get(type_id,
                 (self.default_encode, self.default_decode))[0](obj, buffer)

    def decode(self, buffer):
        type_id = buffer.read_int8()
        return self.codec_map \
            .get(type_id,
                 (self.default_encode, self.default_decode))[1](buffer)

    def serialize(self, obj):
        import pyfury as fury
        buffer = fury.Buffer.allocate(16)
        self.encode(obj, buffer)
        return buffer.get_binary(0, buffer.writer_index)

    def deserialize(self, data):
        import pyfury as fury
        buffer = fury.Buffer(data)
        return self.decode(buffer)


__all__ = ["Serializer", "PythonSerializer", "CrossLangSerializer"]
