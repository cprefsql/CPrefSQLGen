#!/usr/bin/python -u
# -*- coding: utf-8 -*-

import os
import gen
import file_handler
import csv

'''
CPrefSQLGen:
Dataset Generator with Simple Preferences for CprefSQL
'''

MAIN_DIR = 'experiments'
DETAILS_DIR = MAIN_DIR + os.sep + 'details'
RUNTIME_SUMMARY_DIR = MAIN_DIR + os.sep + 'runtime_summary'
MEMORY_SUMMARY_DIR = MAIN_DIR + os.sep + 'memory_summary'
RUNTIME_RESULT_DIR = MAIN_DIR + os.sep + 'runtime_result'
MEMORY_RESULT_DIR = MAIN_DIR + os.sep + 'memory_result'
RULES_DIR = MAIN_DIR + os.sep + 'queries'
DATA_DIR = MAIN_DIR + os.sep + 'data'

DIR_LIST = [MAIN_DIR, DETAILS_DIR, RUNTIME_SUMMARY_DIR, MEMORY_SUMMARY_DIR,
            RUNTIME_RESULT_DIR, MEMORY_RESULT_DIR, RULES_DIR, DATA_DIR]

# List of attributes number for testing
ATTRIBUTE_LIST = [8, 16, 32, 64]
# Default attributes number
ATTRIBUTE_DEFAULT = 8

# List of tuples number
TUPLE_LIST = [50000, 100000, 200000, 400000, 800000]
# default tuple number
TUPLE_DEFAULT = 200000

# List of tuples number
TUPLE_LIST_BNL = [500, 1000, 2000, 4000, 8000]
# default tuple number
TUPLE_DEFAULT_BNL = 1000

# Parameter values related to preference queries
# List of rules number (8)
RULE_LIST = [2, 4, 8, 16, 32]
# Default rules number
RULE_DEFAULT = 8

# List of levels
LEVEL_LIST = [1, 2, 4, 8]
# Default level
LEVEL_DEFAULT = 2

# Indifferent attributes list
INDIFF_LIST = [0, 1, 2, 4, 5]
# Default indifferent attributes
INDIFF_DEFAULT = 5

# Top-k variation (-1 for best operator)
TOPK_LIST = [-1, 12500, 25000, 50000, 100000, 200000]
# Default Top-k number
TOPK_DEFAULT = -1

# Bases
SQLITE = 'sqlite'
POSTGRESQL = 'postgresql'
BASE_LIST = [SQLITE, POSTGRESQL]

# Experiment parameters
ATT = 'att'
TUP = 'tup'
RUL = 'rul'
LEV = 'lev'
IND = 'ind'
TOP = 'top'
ALG = 'alg'

REW_DEFAULT = False

# Algorithms for tuples comparison
TUP_ALG_PARTITION = 'partition'
TUP_ALG_BNL_STAR_STAR = 'bnl'
TUP_ALG_MAX_PREF = 'maxpref'

# List of algorithms for tuples
TUP_ALG_LIST = [TUP_ALG_PARTITION, TUP_ALG_MAX_PREF]
# Default algorithm for tuples
TUP_DEFAULT_ALG = TUP_ALG_MAX_PREF

# List of algorithms for tuples for BNL test
TUP_ALG_LIST_BNL = [TUP_ALG_PARTITION, TUP_ALG_MAX_PREF, TUP_ALG_BNL_STAR_STAR]
# Default algorithm for tuples for BNL test
TUP_DEFAULT_ALG_BNL = TUP_ALG_BNL_STAR_STAR

EXPERIMENT_RERUN = 2
RUNTIME = "runtime"
MEMORY = "memory"

# Confidense Interval
CONFIDENCE_INTERVAL = 0.95


def get_query_id(n_rules, level, ind, top):
    '''
    Return a query ID for given parameters
    '''
    operation = 'best'
    if top != -1:
        operation = TOP + str(top)

    return RUL + str(n_rules) + \
        LEV + str(level) + \
        IND + str(ind) + \
        operation


def get_experiment_id(exp_conf):
    '''
    Return the ID of an experiment
    '''
    return get_table_id(exp_conf[TUP], exp_conf[ATT]) + get_query_id(exp_conf[RUL],exp_conf[LEV],exp_conf[IND],exp_conf[TOP])


def get_table_id(tup_number, att_number):
    '''
    Return a table ID for given parameters
    '''
    return ATT + str(att_number) + \
        TUP + str(tup_number)


def get_input_file(tupples, attributes):
    '''
    Get filename for experiment input data
    '''
    table_id = get_table_id(tupples, attributes)
    filename = DATA_DIR + os.sep + table_id + '.csv'
    return filename


def get_rule_file(rules, level, indifferent, topk):
    '''
    Get filename for experiment rules
    '''
    query_id = get_query_id(rules, level, indifferent, topk)
    filename = RULES_DIR + os.sep + query_id + '.txt'
    return filename


def get_detail_file(algorithm, experiment_id, count, base):
    '''
    Get filename for experiment details
    '''
    return DETAILS_DIR + os.sep + base + os.sep + algorithm + os.sep + experiment_id + '.' + str(count) + '.csv'


def add_experiment(experiment_list, experiment):
    '''
    Add an experiment into experiment list
    '''
    if experiment not in experiment_list:
        experiment_list.append(experiment.copy())


def gen_experiment_list():
    '''
    Generate the list of experiments
    '''
    exp_list = []
    # Default parameters
    def_rec = {ALG: TUP_DEFAULT_ALG, ATT: ATTRIBUTE_DEFAULT,
               TUP: TUPLE_DEFAULT, RUL: RULE_DEFAULT, LEV: LEVEL_DEFAULT,
               IND: INDIFF_DEFAULT, TOP: TOPK_DEFAULT}

    # Standard experiments
    for alg in TUP_ALG_LIST:
        def_rec[ALG] = alg

        # Attributes number variation
        for att_number in ATTRIBUTE_LIST:
            rec = def_rec.copy()
            rec[ATT] = att_number
            add_experiment(exp_list, rec)

        # Tuples number variation
        for tup_number in TUPLE_LIST:
            rec = def_rec.copy()
            rec[TUP] = tup_number
            add_experiment(exp_list, rec)

        # Rules number variation
        for rules_number in RULE_LIST:
            rec = def_rec.copy()
            rec[RUL] = rules_number
            add_experiment(exp_list, rec)

        # Level variation
        for level in LEVEL_LIST:
            rec = def_rec.copy()
            rec[LEV] = level
            add_experiment(exp_list, rec)

        # indifferent attributes variation
        for indif_number in INDIFF_LIST:
            rec = def_rec.copy()
            rec[IND] = indif_number
            add_experiment(exp_list, rec)

        # topk variation
        for topk_number in TOPK_LIST:
            rec = def_rec.copy()
            rec[TOP] = topk_number
            add_experiment(exp_list, rec)

    # BNL experiments
    def_rec = {ALG: TUP_DEFAULT_ALG_BNL, ATT: ATTRIBUTE_DEFAULT,
               TUP: TUPLE_DEFAULT_BNL,
               RUL: RULE_DEFAULT, LEV: LEVEL_DEFAULT, IND: INDIFF_DEFAULT,
               TOP: TOPK_DEFAULT}
    for alg in TUP_ALG_LIST_BNL:
        def_rec[ALG] = alg

        # Tuples number variation
        for tup_number in TUPLE_LIST_BNL:
            rec = def_rec.copy()
            rec[TUP] = tup_number
            add_experiment(exp_list, rec)

    return exp_list


def initialize_directories():
    print("creating directories...")
    for directory in DIR_LIST:
        file_handler.create_directory(directory)

    for directory in BASE_LIST:
        file_handler.create_directory(DETAILS_DIR+os.sep+directory)
        for alg in TUP_ALG_LIST_BNL:
            file_handler.create_directory(DETAILS_DIR+os.sep+directory+os.sep+alg)


def create_experiments(exp_list):
    # Generate directories
    initialize_directories()

    # Generate all data files
    for exp in exp_list:
        # generate tupples
        recs = gen.gen_records(exp[TUP], exp[ATT])
        # create csv file
        attribute_list = list(recs[0].keys())
        table_id = get_table_id(exp[TUP], exp[ATT])
        filename = DATA_DIR + os.sep + table_id + '.csv'
        # store in csv file
        file_handler.write_to_csv(filename, attribute_list, recs)

    # Generate Preference Rule Files
    for exp in exp_list:
        # generate rules
        rules = gen.gen_rules(exp[RUL], exp[LEV], exp[IND])
        prefs = '\nAND\n'.join(rules)

        # create text file
        query_id = get_query_id(exp[RUL], exp[LEV], exp[IND], exp[TOP])
        filename = RULES_DIR + os.sep + query_id + '.txt'	
        # store in text file
        file_handler.write_to_txt(filename, prefs)


def run(experiment_conf, count, base):
    '''
    Run experiment with parameters
    '''

    RUN_COMMAND_SQLITE = "python3 cprefsql_sqlite.py -d {d} -i {i} -r {r} -a {a} -t {t}"
    RUN_COMMAND_POSTGRESQL = "python3 cprefsql_postgresql.py -d {d} -i {i} -r {r} -a {a} -t {t}"
    experiment_id = get_experiment_id(experiment_conf)

    algorithm = experiment_conf[ALG]
    detail_file = get_detail_file(algorithm, experiment_id, count, base)
    input_file = get_input_file(experiment_conf[TUP], experiment_conf[ATT])
    rule_file = get_rule_file(experiment_conf[RUL], experiment_conf[LEV],
                              experiment_conf[IND], experiment_conf[TOP])
    topk = experiment_conf[TOP]

    if base == SQLITE:
        if not os.path.isfile(detail_file):
            command = RUN_COMMAND_SQLITE.format(d=detail_file,
                                                i=input_file, r=rule_file,
                                                a=algorithm, t=topk)
            print("running sql: ", command)
            os.system(command)
    elif base == POSTGRESQL:
        if not os.path.isfile(detail_file):
            command = RUN_COMMAND_POSTGRESQL.format(d=detail_file,
                                                    i=input_file, r=rule_file,
                                                    a=algorithm, t=topk)
            print("running postgresql: ", command)
            os.system(command)


def run_experiments(experiment_list):
    '''
    Run all experiment
    '''
    repetition = EXPERIMENT_RERUN + 1

    for base in BASE_LIST:
        for exp_rec in experiment_list:
            for count in range(repetition):
                run(exp_rec, count + 1, base)


def calculate_confidence_interval(confidence, values):
    '''
    Calculate statistics
    '''
    from scipy.stats import norm

    fsum = sum(values)
    fcount = len(values)
    # Calculate mean
    fmean = fsum / fcount

    # Calculate variance
    fvariance = 0
    for value in values:
        fvariance += (value - fmean) ** 2
    fvariance = fvariance / (fcount - 1)

    # Calculate standard deviation
    fstd_deviantion = fvariance ** 0.5

    # Calculate critical z
    critical_z = 1 - ((1 - confidence) / 2)
    critical_z = norm.ppf(critical_z)
    # Calculate confidence interval
    fconf_interval = critical_z * fstd_deviantion / fcount ** 0.5

    return fconf_interval, fmean


def summarize_confidence_interval(detail_file_list):
    mem_list = []
    time_list = []

    for file_name in detail_file_list:
        # print("File: ", file)
        if not os.path.isfile(file_name):
            print('File does not exists: ' + file_name)
            return

        file_conf = open(file_name, 'r')
        reader = csv.DictReader(file_conf, skipinitialspace=True)

        for rec in reader:
            time_list.append(float(rec[RUNTIME]))
            mem_list.append(float(rec[MEMORY]))

    mem_conf, mem_mean = calculate_confidence_interval(CONFIDENCE_INTERVAL, mem_list)
    time_conf, time_mean = calculate_confidence_interval(CONFIDENCE_INTERVAL, time_list)

    print("Memory: ", round(mem_mean, 3), " MB ± ", round(mem_conf, 3))
    print("Runtime: ", round(time_mean, 3), " sec ± ", round(time_conf, 3))

    return round(mem_mean,3), round(mem_conf,3), round(time_mean,3), round(time_conf,3)


def summarize_all():
    '''
    Summarize data from all experiments
    '''
    repetition = EXPERIMENT_RERUN + 1

    for base in BASE_LIST:
        print("Experiments on ", base, "\n")

        # Default parameters
        def_rec = {ATT: ATTRIBUTE_DEFAULT, TUP: TUPLE_DEFAULT,
                   RUL: RULE_DEFAULT, LEV: LEVEL_DEFAULT, IND: INDIFF_DEFAULT,
                   TOP: TOPK_DEFAULT}

        print("Experiments varying attribute number: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + ATT + ".csv"
        atributes = [ATT]
        for algorithm in TUP_ALG_LIST:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Attributes number variation
        for att_number in ATTRIBUTE_LIST:
            print("Experiment with ATT = ", att_number)
            algo_results = []
            algo_results.append(att_number)
            for algorithm in TUP_ALG_LIST:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[ATT] = att_number
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id, count_file, base)  
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])

        print("\nExperiments varying tupple number: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + TUP + ".csv"
        atributes = [TUP]
        for algorithm in TUP_ALG_LIST:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Tupple number variation
        for tup_number in TUPLE_LIST:
            print("Experiment with TUP = ", tup_number)
            algo_results = []
            algo_results.append(tup_number)
            for algorithm in TUP_ALG_LIST:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[TUP] = tup_number
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id, count_file, base)
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])

        print("\nExperiments varying rule number: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + RUL + ".csv"
        atributes = [RUL]
        for algorithm in TUP_ALG_LIST:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Rule number variation
        for rules_number in RULE_LIST:
            print("Experiment with RUL = ", rules_number)
            algo_results = []
            algo_results.append(rules_number)
            for algorithm in TUP_ALG_LIST:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[RUL] = rules_number
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id,
                                                  count_file, base)
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])

        print("\nExperiments varying level number: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + LEV + ".csv"
        atributes = [LEV]
        for algorithm in TUP_ALG_LIST:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Rule number variation
        for level in LEVEL_LIST:
            print("Experiment with LEV = ", level)
            algo_results = []
            algo_results.append(level)
            for algorithm in TUP_ALG_LIST:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[LEV] = level
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id,
                                                  count_file, base)
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])

        print("\nExperiments varying indifferent number: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + IND + ".csv"
        atributes = [IND]
        for algorithm in TUP_ALG_LIST:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Rule number variation
        for indif_number in INDIFF_LIST:
            print("Experiment with IND = ", indif_number)
            algo_results = []
            algo_results.append(indif_number)
            for algorithm in TUP_ALG_LIST:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[IND] = indif_number
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id, count_file, base)  
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])

        print("\nExperiments varying topk number: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + TOP + ".csv"
        atributes = [TOP]
        for algorithm in TUP_ALG_LIST:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Rule number variation
        for topk_number in TOPK_LIST:
            print("Experiment with TOP = ", topk_number)
            algo_results = []
            algo_results.append(topk_number)
            for algorithm in TUP_ALG_LIST:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[TOP] = topk_number
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id,
                                                  count_file, base)
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])

    # Experiments with BNL
    for base in BASE_LIST:
        print("Experiments on ", base, "\n")

        # Default parameters
        def_rec = {ATT: ATTRIBUTE_DEFAULT, TUP: TUPLE_DEFAULT_BNL,
                   RUL: RULE_DEFAULT, LEV: LEVEL_DEFAULT, IND: INDIFF_DEFAULT,
                   TOP: TOPK_DEFAULT}

        print("\nExperiments varying tupple number and BNL: ")
        filename = RUNTIME_SUMMARY_DIR + os.sep + base + "_" + TUP + "BNL" + ".csv"
        atributes = [TUP]
        for algorithm in TUP_ALG_LIST_BNL:
            atributes.append(algorithm+"memmean")
            atributes.append(algorithm+"memconf")
            atributes.append(algorithm+"timemean")
            atributes.append(algorithm+"timeconf")
        file_handler.write_to_csv(filename, atributes, [])

        # Tupple number variation
        for tup_number in TUPLE_LIST_BNL:
            print("Experiment with TUP = ", tup_number)
            algo_results = []
            algo_results.append(tup_number)
            for algorithm in TUP_ALG_LIST_BNL:
                print("Experiments with algorithm: ", algorithm, "\n")

                exp_rec = def_rec.copy()
                exp_rec[TUP] = tup_number
                experiment_id = get_experiment_id(exp_rec)

                detail_file_list = []
                for count in range(repetition):
                    count_file = count+1
                    detail_file = get_detail_file(algorithm, experiment_id,
                                                  count_file, base)
                    print("detail_file: ", detail_file)
                    detail_file_list.append(detail_file)

                mem_mean, mem_conf, time_mean, time_conf = summarize_confidence_interval(detail_file_list)

                algo_results.append(mem_mean)
                algo_results.append(mem_conf)
                algo_results.append(time_mean)
                algo_results.append(time_conf)

            partial_results = {}

            for i, _ in enumerate(algo_results):
                partial_results[atributes[i]] = algo_results[i]
            file_handler.append_to_csv(filename, atributes, [partial_results])


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('CPrefSQLGen')
    parser.add_argument('-g', '--gen', action="store_true",
                        default=False,
                        help='Generate files')
    parser.add_argument('-r', '--run', action="store_true",
                        default=False,
                        help='Run experiments')
    parser.add_argument('-s', '--summarize', action="store_true",
                        default=False,
                        help='Summarize results')
    args = parser.parse_args()
    if print_help:
        parser.print_help()
    return args


def main():
    '''
    Main routine
    '''
    args = get_arguments()

    # Create experiments
    exp_list = gen_experiment_list()

    if args.gen:
        # generating data
        print('Generating data')
        print('Generating queries')
        create_experiments(exp_list)
    elif args.run:
        print('Running experiments')
        run_experiments(exp_list)
    elif args.summarize:
        print('Summarizing results')
        summarize_all()
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
