import io
import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder

def encode(avro_schema_path, value):
   
    # Get the schema to use to serialize the message
    schema = avro.schema.parse(open(avro_schema_path, "rb").read())
    writer = DatumWriter(schema)

    # serialize the message data using the schema
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)

    writer.write(value, encoder)

    return bytes_writer.getvalue()

def decode(avro_schema_path, raw_bytes):
    schema = avro.schema.parse(open(avro_schema_path, "rb").read())

    bytes_reader = io.BytesIO(raw_bytes)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    result = reader.read(decoder)   
    return result

