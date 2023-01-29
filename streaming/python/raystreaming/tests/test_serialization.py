from raystreaming.runtime.serialization import CrossLangSerializer
from raystreaming.message import Record, KeyRecord, ObjectRow


def test_serialize():
    serializer = CrossLangSerializer()
    record = Record("value")
    record.stream = "stream1"
    key_record = KeyRecord("key", "value")
    key_record.stream = "stream2"
    assert record == serializer.deserialize(serializer.serialize(record))
    assert key_record == serializer.\
        deserialize(serializer.serialize(key_record))


def test_kepler_row_serialize():
    serializer = CrossLangSerializer()
    row = ObjectRow(2)
    row.set_field(0, "1")
    row.set_field(1, 2)
    assert row == serializer.deserialize(serializer.serialize(row))


def test_serialize_arrow_record_batch():
    import pyarrow as pa
    serializer = CrossLangSerializer()
    data = [
        pa.array([1, 2, 3, 4]),
        pa.array(["foo", "bar", "baz", None]),
        pa.array([True, None, False, True])
    ]
    batch = pa.record_batch(data, names=["f0", "f1", "f2"])
    assert batch == serializer.deserialize(serializer.serialize(batch))
