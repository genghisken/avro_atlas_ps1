"""
Validate and read messages based on the ATLAS Avro schema. Note that
The schema files should be send order of lowest to highest in terms of
hierarchy.

The code is based on originali LSST code by M. Patterson which can be found at:
https://github.com/lsst-dm/sample-avro-alert

Usage:
  %s <messageFile> <schema>... [--schemalessMessage]
  %s (-h | --help)
  %s --version

Options:
  -h --help                     Show this screen.
  --version                     Show version.
  --schemalessMessage           Apply the input schema to the (single) message. Otherwise read the schema encoded in the message.

Example:
  python %s alert.avro ../schema/cutout.avsc ../schema/candidate.avsc ../schema/alert.avsc
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
from gkutils import Struct, cleanOptions
from avroutils import combine_schemas, load_single_avsc, load_stamp, write_stamp_file, write_avro_data, read_avro_data, read_avro_data_bulk, read_avro_data_from_file, check_md5
import json

def printMessage(m):
    # Print message text to screen
    message_text = {k: m[k] for k in m if k not in [
        'cutoutScience', 'cutoutDifference', 'cutoutTemplate']}
    print(message_text)

    # Collect stamps as files written to local directory 'output' and check hashes match expected
    if m.get('cutoutScience') is not None:
        stamp_temp_out = write_stamp_file(message.get('cutoutScience'), 'output')
        print('Science stamp ok:', check_md5(args.cutoutSci, stamp_temp_out))

    if m.get('cutoutTemplate') is not None:
        stamp_temp_out = write_stamp_file(message.get('cutoutTemplate'), 'output')
        print('Template stamp ok:', check_md5(args.cutoutTemp, stamp_temp_out))

    if m.get('cutoutDifference') is not None:
        stamp_diff_out = write_stamp_file(message.get('cutoutDifference'), 'output')

    print("size in bytes of json text: %d" % sys.getsizeof(message_text))
    #raw_bytes = avro_bytes.getvalue()
    #print("size in bytes of avro message: %d" % sys.getsizeof(raw_bytes))


def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    alert_schema = combine_schemas(options.schema)

    if options.schemalessMessage:
        m = read_avro_data_from_file(options.messageFile, alert_schema)
        printMessage(m)
    else:
        data = read_avro_data_bulk(options.messageFile)

        if data:
            for m in data:
                printMessage(m)

    return 0


if __name__ == "__main__":
    main()
