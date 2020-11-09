#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Running module for SQLite testing
'''

# Manager imports
import os
import sys
import csv
import sqlite3
import time
import psutil
import resource

# Required to relative package imports
sys.path.append("/home/lucas/trabalho/cprefsql")

from algorithms.partition import get_best_partition, get_topk_partition
from algorithms.nested_loops import get_best, get_topk
from algorithms.maxpref import get_mbest_partition, get_mtopk_partition

# Algorithms for tuples comparison
TUP_ALG_PARTITION = 'partition'
TUP_ALG_BNL_STAR_STAR = 'bnl'
TUP_ALG_MAX_PREF = 'maxpref'

DATA_FILE = "rdb.db"

# List of algorithms for tuples
TUP_ALG_LIST = [TUP_ALG_PARTITION, TUP_ALG_BNL_STAR_STAR, TUP_ALG_MAX_PREF]
# Default algorithm for tuples
TUP_DEFAULT_ALG = TUP_ALG_BNL_STAR_STAR

###############################################################################
# Other configuration

# Field separator configuration
DEFAULT_DELIMITER = ','

# Iterations number configuration
DEFAULT_MAX_TIMESTAMP = 10

# Default lines number to read from files
DEFAULT_LINES_NUMBER_READ = 100000

# Default log file
DEFAULT_LOG_FILE = 'cprefsqllog.log'


def read_tuples_to_insert(filename):
    if not os.path.isfile(filename):
        print('File does not exists: ' + filename)
        return ""

    file_tuples = open(filename, 'r')
    reader = csv.DictReader(file_tuples, skipinitialspace=True)

    recs = []
    for rec in reader:
        recs.append(rec)

    return reader.fieldnames.copy(), recs


def build_database(fieldnames, datalist):

    cleaner_query = "DROP TABLE IF EXISTS r;"
    create_query = "CREATE TABLE r ( {f} );"
    insert_query = "INSERT INTO r VALUES \n{list};"

    # create table
    field_list = " , ".join([s + " INT " for s in fieldnames])
    create_query = create_query.format(f=field_list)
    datalist_values = []
    for rec in datalist:
        datalist_values.append("("+" , ".join(list(rec.values()))+")")
    full_list = " ,\n".join(datalist_values)
    insert_query = insert_query.format(list=full_list)

    conn = None
    try:
        conn = sqlite3.connect(DATA_FILE)
    except Error as e:
        print(e)

    if conn is not None:
        print("Constructing test database...")
        try:
            cursor = conn.cursor()
            cursor.execute(cleaner_query)
            conn.commit()
            cursor.execute(create_query)
            conn.commit()
            cursor.execute(insert_query)
            conn.commit()
            print("Done!")
        except Error as e:
            print(e)

        conn.close()


def dump_database():
    search_query = "SELECT * FROM r;"
    conn = None
    try:
        conn = sqlite3.connect(DATA_FILE)
        # conn.row_factory = sqlite3.Row
    except Error as e:
        print(e)

    if conn is not None:
        print("Dumping database...")
        cursor = conn.cursor()
        cursor.execute(search_query)

        print('\n\nDatabase records:')
        for rec in cursor.fetchall():
            print(rec)
            # print(dict(rec))

        conn.close()


def get_arguments():
    '''
    Get arguments
    '''
    import argparse

    parser = argparse.ArgumentParser('CPrefSQL')
    parser.add_argument('-d', '--details', default=None,
                        help='Append execution details to file')
    parser.add_argument('-i', '--input', default=None,
                        help='CSV input file')
    parser.add_argument('-r', '--rules', default=None,
                        help='CPrefSQL rule file')
    parser.add_argument('-a', '--algo', choices=TUP_ALG_LIST,
                        default=TUP_DEFAULT_ALG,
                        help='Preference algorithm')
    parser.add_argument('-t', '--topk', default=-1,
                        help='Number of TopK tupples')

    args = parser.parse_args()
    return args


def main():
    '''
    Main CPrefSQL function
    '''

    # Get arguments
    args = get_arguments()

    input_file = args.input
    details_file = args.details
    pref_filename = args.rules
    algorithm = args.algo
    topk = int(args.topk)

    print("Parameters: ", input_file, " ", details_file, " ",
          pref_filename, " ", algorithm, " ", topk)

    fields, recs = read_tuples_to_insert(input_file)
    build_database(fields, recs)
    # dump_database()

    pref_file = open(pref_filename)
    pref_text = pref_file.read()

    print('\n\nPreferences:')
    print(pref_text)
    print("Input entries loaded and ready!")

    query = "SELECT * FROM r;"
    rec_list = []

    print("Running query...")
    # To measure run time
    start_time = time.time()

    CON = sqlite3.connect(DATA_FILE)
    CON.row_factory = sqlite3.Row
    CURSOR = CON.cursor()
    CURSOR.execute(query)
    for rec in CURSOR.fetchall():
        rec_list.append(dict(rec))

    if topk < 0:
        # Run BEST algorithm
        if algorithm == TUP_ALG_BNL_STAR_STAR:
            BEST_LIST = get_best(pref_text, rec_list)
        elif algorithm == TUP_ALG_PARTITION:
            BEST_LIST = get_best_partition(pref_text, rec_list)
        elif algorithm == TUP_ALG_MAX_PREF:
            BEST_LIST = get_mbest_partition(pref_text, rec_list)
    else:
        # Run TOPK algorithm
        if algorithm == TUP_ALG_BNL_STAR_STAR:
            BEST_LIST = get_topk(pref_text, rec_list, topk)
        elif algorithm == TUP_ALG_PARTITION:
            BEST_LIST = get_topk_partition(pref_text, rec_list, topk)
        elif algorithm == TUP_ALG_MAX_PREF:
            BEST_LIST = get_mtopk_partition(pref_text, rec_list, topk)

    # To measure run time
    end_time = time.time()

    # to get memory
    # must change by OS, on UNIX uses "resource" library
    # process = psutil.Process(os.getpid())
    # mem = round(process.memory_info().peak_wset / 1024.0 / 1024.0, 3)
    mem = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 3)
    runtime = round(end_time - start_time, 3)

    print("runtime: ", runtime)
    print("memory: ", mem)
    print('Best List size: ', len(BEST_LIST))

    # Storing performance data
    out_file = open(details_file, 'w')
    out_write = csv.DictWriter(out_file, ['timestamp', 'runtime', 'memory'])
    out_write.writeheader()
    rec = {'timestamp': time.time(), 'runtime': runtime, 'memory': mem}
    out_write.writerow(rec)
    out_file.close()


if __name__ == '__main__':
    main()
    # run as python3 cprefsql_sqlite.py -d
    # detal.csv -i input.csv -r pref.txt -a bnl -t -1
