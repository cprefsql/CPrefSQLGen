#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Main module
'''

# Manager imports
import os
import sys
import csv

import psycopg2
import psycopg2.extras

import time


# Algorithms for tuples comparison
TUP_ALG_PARTITION = 'partition'
TUP_ALG_BNL_STAR_STAR = 'bnl'
TUP_ALG_MAX_PREF = 'maxpref'

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

# PostgreSQL connecton variables
host = "localhost"
database = "trabalho"
user = "lion"
password = "natani"


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

    cleaner_query = "DROP TABLE IF EXISTS r CASCADE;"
    create_query = "CREATE TABLE r ( {f} );"
    insert_query = "INSERT INTO r VALUES \n{list};"

    # create table
    field_list = " , ".join([s + " INT " for s in fieldnames])
    create_query = create_query.format(f=field_list)
    print('CREATE QUERY: ', create_query)
    datalist_values = []
    for rec in datalist:
        datalist_values.append("("+" , ".join(list(rec.values()))+")")
    full_list = " ,\n".join(datalist_values)
    insert_query = insert_query.format(list=full_list)

    # load functions
    function_filename = "functions.sql"
    function_file = open(function_filename, "r")
    functions_query = function_file.read()

    conn = None
    try:
        conn = psycopg2.connect(host=host, database=database,
                                user=user, password=password)
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
            cursor.execute(functions_query)
            conn.commit()
            print("Done!")
        except Error as e:
            print(e)

        conn.close()


def dump_database():
    search_query = "SELECT * FROM r;"
    conn = None
    try:
        conn = psycopg2.connect(host=host, database=database,
                                user=user, password=password)
    except Error as e:
        print(e)

    if conn is not None:
        print("Dumping database...")
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(search_query)

        print('\n\nDatabase records:')
        for rec in cursor.fetchall():
            print(rec)

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

    # command line
    # python cprefsql_postgre.py -d detail.csv
    #    -i input.csv -r pref.txt -a partition -t -1

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

    pref_file = open(pref_filename)
    pref_text = pref_file.read()

    query = ""
    rec_list = []

    print('\n\nPreferences:')
    print(pref_text)

    # Dump the database for debug
    # dump_database()

    print("Running query...")

    if topk < 0:
        # Run BEST algorithm

        basic_query = "SELECT * FROM {func}( '{pref}');"

        if algorithm == TUP_ALG_BNL_STAR_STAR:
            basic_query = basic_query.format(func="best_bnl",
                                             pref=pref_text)
        elif algorithm == TUP_ALG_PARTITION:
            basic_query = basic_query.format(func="best_partition",
                                             pref=pref_text)
        elif algorithm == TUP_ALG_MAX_PREF:
            basic_query = basic_query.format(func="best_maxpref",
                                             pref=pref_text)

    else:
        # Run TOPK algorithm

        basic_query = "SELECT * FROM {func}( '{pref}', {topk} );"

        if algorithm == TUP_ALG_BNL_STAR_STAR:
            basic_query = basic_query.format(func="topk_bnl",
                                             pref=pref_text, topk=topk)
        elif algorithm == TUP_ALG_PARTITION:
            basic_query = basic_query.format(func="topk_partition",
                                             pref=pref_text, topk=topk)
        elif algorithm == TUP_ALG_MAX_PREF:
            basic_query = basic_query.format(func="topk_maxpref",
                                             pref=pref_text, topk=topk)

    conn = psycopg2.connect(host=host,
                            database=database, user=user, password=password)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # To measure run time
    start_time = time.time()

    cursor.execute(basic_query)
    rec_list = []
    for rec in cursor.fetchall():
        rec_list.append(rec)

    # To measure run time
    end_time = time.time()

    # print('\n\nBest records:')
    # for rec in rec_list:
    #    print (rec)

    conn.close()

    runtime = round(end_time - start_time, 3)

    print("runtime: ", runtime)
    print("memory: ", 0)
    print('Best List size: ', len(rec_list))

    # Storing performance data
    out_file = open(details_file, 'w')
    out_write = csv.DictWriter(out_file, ['timestamp', 'runtime', 'memory'])
    out_write.writeheader()
    rec = {'timestamp': time.time(), 'runtime': runtime, 'memory': 0}
    out_write.writerow(rec)
    out_file.close()


if __name__ == '__main__':
    main()
