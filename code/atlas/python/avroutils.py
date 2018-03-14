# The code is based on code by M. Patterson which can be found at:
# https://github.com/lsst-dm/sample-avro-alert
import avro.schema
import fastavro
import sys
import io
import json
import argparse
import os.path
import hashlib

def combine_schemas(schema_files):
    """Combine multiple nested schemas into a single schema.
    """
    known_schemas = avro.schema.Names()

    for s in schema_files:
        schema = load_single_avsc(s, known_schemas)
    return schema.to_json()

def load_single_avsc(file_path, names):
    """Load a single avsc file.
    """
    with open(file_path) as file_text:
        json_data = json.load(file_text)
    schema = avro.schema.SchemaFromJSONData(json_data, names)
    return schema

def load_stamp(file_path):
    """Load a cutout postage stamp file to include in alert.
    """
    _, fileoutname = os.path.split(file_path)
    with open(file_path, mode='rb') as f:
        cutout_data = f.read()
        cutout_dict = {"fileName": fileoutname, "stampData": cutout_data}
    return cutout_dict

def write_stamp_file(stamp_dict, output_dir):
    """Given a stamp dict that follows the cutout schema, write data to a file in a given directory.
    """
    filename = stamp_dict['fileName']
    try:
        os.makedirs(output_dir)
    except OSError:
        pass
    out_path = os.path.join(output_dir, filename)
    with open(out_path, 'wb') as f:
        f.write(stamp_dict['stampData'])
    return out_path

def write_avro_data(json_data, json_schema):
    """Encode json with fastavro module into avro format given a schema.
    """
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, json_schema, json_data)
    return bytes_io

def read_avro_data(bytes_io, json_schema):
    """Read avro data with fastavro module and decode with a given schema.
    """
    bytes_io.seek(
        0)  # force schemaless_reader to read from the start of stream, byte offset = 0
    message = fastavro.schemaless_reader(bytes_io, json_schema)
    return message

def read_avro_data_from_file(filename, json_schema):
    """Read avro data from a file with fastavro module and decode with a given schema.
       Note that this only works for a single record!
    """
    message = None
    with open(filename, 'rb') as f:
        data = io.BytesIO(f.read())
        message = fastavro.schemaless_reader(data, json_schema)
    return message

def read_avro_data_bulk(filename, schema=None):
    """Read avro data from a file with fastavro module and decode it.
       The schema is assumed to be present in the file.
    """
    data = []
    with open(filename, 'rb') as f:
        avro_reader = fastavro.reader(f, reader_schema = schema)
        for record in avro_reader:
            data.append(record)
    return data

def write_avro_data_to_file_with_schema(filename, json_schema, records):
    """Write out large numbers of records with the schema.
       This makes for easier reading and does not significantly affect space.
    """
    with open(filename, 'wb') as out:
        fastavro.writer(out, json_schema, records)

def check_md5(infile, outfile):
    """Compare the MD5 hash values of two files.
    """
    with open(infile, 'rb') as f:
        in_data = f.read()
        in_md5 = hashlib.md5(in_data).hexdigest()
    with open(outfile, 'rb') as f:
        out_data = f.read()
        out_md5 = hashlib.md5(out_data).hexdigest()
    return in_md5 == out_md5
