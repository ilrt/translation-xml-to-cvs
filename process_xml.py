"""
A script that processes the Qualitivity XML files and creates CSV files of extracted data.
"""

import argparse
import os
import sys
from xml.etree import ElementTree

import numpy as np
import pandas as pd

# data frame columns
columns = ['Record ID', 'Segment ID', 'Total pause duration_300', 'Pause count_300',
           'Total pause duration_500', 'Pause count_500', 'Total pause duration_1s', 'Pause count_1s',
           'Keystrokes', 'Active ms', 'Record duration', 'Total duration']

# date time format used in the XML
DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def normalize_attribute(root):
    """ Make all the attributes lower case since the source XML files are not consistent. """
    for attr, value in root.attrib.items():
        norm_attr = attr.lower()
        if norm_attr != attr:
            root.set(norm_attr, value)
            root.attrib.pop(attr)

    for child in root:
        normalize_attribute(child)


def create_pause_counts_dict():
    """ Dictionary that will hold our pause count and duration value for a <Record/> element in the XML."""
    return {
        'duration_300': 0,
        'count_300': 0,
        'duration_500': 0,
        'count_500': 0,
        'duration_1000': 0,
        'count_1000': 0,
        'total_pause_ms': 0,
        'total_duration': 0
    }


def ms(val):
    """ Turn a float value into milliseconds as an integer. """
    return int(val * 1000)


def categorize_pause(counts, pause_ms):
    """
    The method that updates the count and duration values.
    :param counts:      the dict that holds our pause count and duration values.
    :param pause_ms:    the pause in milliseconds
    :return:            None.
    """
    if pause_ms >= 300:
        counts['duration_300'] += pause_ms
        counts['count_300'] += 1

    if pause_ms >= 500:
        counts['duration_500'] += pause_ms
        counts['count_500'] += 1

    if pause_ms >= 1000:
        counts['duration_1000'] += pause_ms
        counts['count_1000'] += 1

    counts['total_duration'] += pause_ms


def valid_keystroke(keystroke):
    """ Are we dealing with a valid keystroke? False if its a 'system' keystroke. """
    if keystroke.attrib['origin'] and keystroke.attrib['system'] and not keystroke.attrib['key']:
        return False
    elif not keystroke.attrib['selection'] and not keystroke.attrib['text'] and not keystroke.attrib['key'] and \
            keystroke.attrib['shift'] == 'False' and keystroke.attrib['ctrl'] == 'False' \
            and keystroke.attrib['alt'] == 'False':
        return False
    else:
        return True


def process_file(xml_input):
    """
    The method that updates the count and duration values.
    :param xml_input:   the XML file to be processes.
    :return:            a pandas data frame of data extracted from the xml.
    """
    # empty data structure for the data
    categorized_data = []

    # keep track of all pauses
    all_pauses_data = []

    if not os.path.isfile(xml_input):
        raise ValueError('{} is not a file'.format(xml_input))

    # parse the document and get the root element
    doc_tree = ElementTree.parse(xml_input)
    root = doc_tree.getroot()

    # make attributes lower case - source XML not consistent
    normalize_attribute(root)

    # find all the <Record/> elements
    records = root.findall('.//Document/Record')

    # go through the records, each will be a row in the CVS file
    for record in records:

        # get the date/time that the record data started
        record_started = record.attrib['started']
        record_started_dt = np.datetime64(record_started)

        # get the date/time that the record data stopped
        record_ended = record.attrib['stopped']
        record_ended_dt = np.datetime64(record_ended)

        # calculate the duration of the work on the record in milliseconds
        duration_dt = record_ended_dt - record_started_dt
        duration_ms = duration_dt.astype(int)

        # we track 'milestones', i.e. where the last operation ended
        last_milestone = record_started_dt

        # values we want from the <Record/> attribute
        record_id = record.attrib['id']
        segment_id = record.attrib['segmentid']
        active_ms = record.attrib['activemiliseconds']

        # calculate pauses
        pause_counts = create_pause_counts_dict()

        # get all the keystrokes for a record
        keystrokes = record.findall('.//ks')

        # count all the keystrokes
        keystrokes_count = len(keystrokes)

        valid_keystroke_count = 0

        if keystrokes_count == 0:
            categorize_pause(pause_counts, duration_ms)
            all_pauses_data.append([record_id, segment_id, duration_ms, 'No ks'])
        elif keystrokes_count == 1 and not valid_keystroke(keystrokes[0]):
            categorize_pause(pause_counts, duration_ms)
            all_pauses_data.append([record_id, segment_id, duration_ms, '1 system ks omitted'])
            keystrokes_count = 0
        else:
            # iterate over the keystrokes to calculate pauses
            for ks in keystrokes:
                # filter out 'system' keystrokes
                if valid_keystroke(ks):
                    # keep track of valid keystrokes
                    valid_keystroke_count += 1
                    created = ks.attrib['created']
                    created_dt = np.datetime64(created)
                    diff = created_dt - last_milestone
                    diff_ms = diff.astype(int)
                    last_milestone = created_dt
                    # categorise
                    categorize_pause(pause_counts, diff_ms)
                    # not categorised, for the audit
                    all_pauses_data.append([record_id, segment_id, diff_ms, ''])
                else:
                    all_pauses_data.append([record_id, segment_id, None, 'Omitted ks'])

            if valid_keystroke_count > 0:
                # calculate the pause between the last keystroke and when the record stopped.
                last_pause_dt = record_ended_dt - last_milestone
                last_pause_ms = last_pause_dt.astype(int)
                categorize_pause(pause_counts, last_pause_ms)
                all_pauses_data.append([record_id, segment_id, last_pause_ms, ''])
                keystrokes_count = valid_keystroke_count

        # create a row of data
        row = [record_id, segment_id, pause_counts['duration_300'], pause_counts['count_300'],
               pause_counts['duration_500'], pause_counts['count_500'], pause_counts['duration_1000'],
               pause_counts['count_1000'], keystrokes_count, active_ms, duration_ms,
               pause_counts['total_duration']]

        # append to 2d array
        categorized_data.append(row)

    # create pandas data frames
    df = pd.DataFrame(data=categorized_data, columns=columns)
    all_df = pd.DataFrame(data=all_pauses_data, columns=['Record ID', 'Segment ID', 'Pause durations', 'Notes'])
    return df, all_df


def process(input_dir, output_dir, combine):
    """
    Process a folder of XML files and create a folder of CSV file or single file with the combined results.
    :param input_dir:   input directory with the source XML files.
    :param output_dir   output directory to save the CSV file.
    :param combine      boolean, (True) to combine the results, and (False) to create separate CSV files
                        for each XML files.
    :return:            a pandas data frame of data extracted from the xml.
    """

    # holds data frames if we are combining
    # into a single output file
    combine_df = []
    all_data_combined_df = []
    omitted_combined_df = []

    # check we have an input folder
    if not os.path.isdir(input_dir):
        print('Input is not a folder. Exiting')
        sys.exit(1)

    # check we have an output folder
    if not os.path.isdir(output_dir):
        print('Output is not a folder, creating it.')
        os.makedirs(output_dir)

    # walk the directory looking for files
    for root, dirs, files in os.walk(input_dir):
        # iterate the files
        for file in files:
            # we are interested in xml files
            if file.endswith('.xml'):
                # process the file and create a data frame
                input_file = os.path.join(root, file)
                df, all_df = process_file(input_file)
                # if we are combining, we want the filename in the data (first column).
                # add the data frame to our temporary array
                if combine:
                    df.insert(0, 'File', file)
                    all_df.insert(0, 'File', file)
                    combine_df.append(df)
                    all_data_combined_df.append(all_df)
                else:
                    # not combining, so create a CSV file for each xml file
                    output_file = os.path.join(output_dir, file.replace('.xml', '.csv'))
                    all_output_file = os.path.join(output_dir, file.replace('.xml', '-audit.csv'))
                    df.to_csv(output_file, index=False)
                    all_df.to_csv(all_output_file, index=False)

    # if we are combining, combine output into two files
    if combine:
        df = pd.concat(combine_df, ignore_index=True)
        df.to_csv(os.path.join(output_dir, 'combined.csv'), index=False)
        all_df = pd.concat(all_data_combined_df, ignore_index=True)
        all_df.to_csv(os.path.join(output_dir, 'combined-audit.csv'), index=False)


if __name__ == "__main__":
    """ Main method that will get arguments on the command line. """

    # define the command line parameters and switches
    parser = argparse.ArgumentParser(description='Process Qualitivity XML files.')
    parser.add_argument('input', type=str, help='folder with the source XML files')
    parser.add_argument('output', type=str, help='folder for the output CSV files')
    parser.add_argument('--combine', required=False, action='store_true',
                        help='Combine the output into a single CSV file')

    # parse and process
    args = parser.parse_args()
    process(args.input, args.output, args.combine)
