"""
Validate and produce messages based on the ATLAS Avro schema. Note that
The schema files should be send order of lowest to highest in terms of
hierarchy.

The code is based on originali LSST code by M. Patterson which can be found at:
https://github.com/lsst-dm/sample-avro-alert

Usage:
  %s <configfile> <schema>... [--cutoutSci=<science.jpg>] [--cutoutTemp=<template.jpg>] [--cutoutDiff=<difference.jpg>] [--data=<file.json>] [--mjdThreshold=<n>] [--writeFile] [--bulkMessage]  [--readMessage]
  %s (-h | --help)
  %s --version

Options:
  -h --help                     Show this screen.
  --version                     Show version.
  --cutoutSci=<science.jpg>     File for science image postage stamp.
  --cutoutTemp=<template.jpg>   File for template image postage stamp.
  --cutoutDiff=<difference.jpg> File for difference image postage stamp.
  --data=<file.json>            Bypass the database connection and just send a JSON data file.                       
  --mjdThreshold=<n>            MJD threshold after which to extract detections [default: 58000].
  --writeFile                   Write the Avro message to a file.
  --readMessage                 Read the message we've just written.
  --bulkMessage                 If writeFile is true, write one giant message - otherwise write a single file per message. (Note, a single file per message will generate thousands of files.)

Example:
  python %s config_atlas.yaml ../schema/cutout.avsc ../schema/candidate.avsc ../schema/alert.avsc  --readMessage --writeFile --bulkMessage
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
from gkutils import Struct, cleanOptions, dbConnect
from avroutils import combine_schemas, load_single_avsc, load_stamp, write_stamp_file, write_avro_data, read_avro_data, read_avro_data_from_file, check_md5, write_avro_data_to_file_with_schema

import json
import MySQLdb


def getATLASIngestedDetections(conn, mjdThreshold):
    """
    This method is required for those command-line passed object IDs. We need to know the associated comments.
    """

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select *
              from atlas_v_detectionsddc_flat
             where mjd >= %s
        """, (mjdThreshold,))
        resultSet = cursor.fetchall()

        cursor.close ()

    except MySQLdb.Error as e:
        print ("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    # We now have the flat list of dicts.  But we need to create a full "alert", which in this case will be a list of dicts of dicts.
    # Note that we can do this later on - after we collect the original records.

#                {"name": "alertId", "type": "long", "doc": "ATLAS alert ID. Could be database insert ID."},
#                {"name": "atlas_object_id", "type": "long", "doc": "Associated ATLAS object ID. See ZTF candid."},
#                {"name": "candidate", "type": "atlas.alert.candidate"},

    return resultSet

def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    json_path = options.data
    schema_files = options.schema
    cutoutsci_path = options.cutoutSci
    cutouttemp_path = options.cutoutTemp
    cutoutdiff_path = options.cutoutDiff
    mjdThreshold = float(options.mjdThreshold)

    alert_schema = combine_schemas(schema_files)

    import yaml
    with open(options.configfile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']


    # Now that we have all the data, we need to construct a properly formed alert - FOR EACH ROW.
    # NOTE - To get this to work, feed it junk test.json data.

    # The alerts can be written to a file - but don't forget, we are using the schemaless writer
    # which means that the schemaless reader MUST be used to read the data!
    alert = None

    # If we just have some json data, process it.  Otherwise read from the database.

    if json_path:
        with open(json_path) as file_text:
            json_data = json.load(file_text)

        avro_bytes = write_avro_data(json_data, alert_schema)

        # Load science stamp if included
        if cutoutsci_path is not None:
            cutoutTemplate = load_stamp(cutoutsci_path)
            json_data['cutoutScience'] = cutoutTemplate

        # Load template stamp if included
        if cutouttemp_path is not None:
            cutoutTemplate = load_stamp(cutouttemp_path)
            json_data['cutoutTemplate'] = cutoutTemplate

        # Load difference stamp if included
        if cutoutdiff_path is not None:
            cutoutDifference = load_stamp(cutoutdiff_path)
            json_data['cutoutDifference'] = cutoutDifference

        if options.writeFile:
            with open('/tmp/alert.avro', 'wb') as f:
                # NOTE - This code writes a schemaless message. To read it we need to pass the schema
                #        to the reader. How we pass this message to Kafka is the next problem to be
                #        resolved.
                avro_bytes.seek(0)
                data = avro_bytes.read()
                f.write(data)

        #m = read_avro_data_from_file('alert.avro', alert_schema)
        m = read_avro_data(avro_bytes, alert_schema)
        if options.readMessage:
            if m:
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
                raw_bytes = avro_bytes.getvalue()
                print("size in bytes of avro message: %d" % sys.getsizeof(raw_bytes))

                print("size in bytes of json text: %d" % sys.getsizeof(message_text))
                raw_bytes = avro_bytes.getvalue()
                print("size in bytes of avro message: %d" % sys.getsizeof(raw_bytes))
        return 0

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    # Connect to the database and read out the ATLAS detections.
    records = getATLASIngestedDetections(conn, mjdThreshold)

    conn.close()

    alerts = []
    if options.writeFile and options.bulkMessage:
        for row in records:
            alert = {'alertId': row['db_det_id'], 'atlas_object_id': row['atlas_object_id'], 'candidate': row}
            alerts.append(alert)
        write_avro_data_to_file_with_schema('/tmp/alerts_bulk.avro', alert_schema, alerts)
        return

    for row in records:
        alert = {'alertId': row['db_det_id'], 'atlas_object_id': row['atlas_object_id'], 'candidate': row}

        avro_bytes = write_avro_data(alert, alert_schema)

        if options.readMessage:
            #m = read_avro_data_from_file('alert.avro', alert_schema)
            m = read_avro_data(avro_bytes, alert_schema)
            if m:
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
                raw_bytes = avro_bytes.getvalue()
                print("size in bytes of avro message: %d" % sys.getsizeof(raw_bytes))

        if options.writeFile:
            if not options.bulkMessage:
                f = open('/tmp/alert_%s.avro' % row['db_det_id'], 'wb')
            avro_bytes.seek(0)
            data = avro_bytes.read()
            f.write(data)
            if not options.bulkMessage:
                f.close()

    return 0


if __name__ == "__main__":
    main()
