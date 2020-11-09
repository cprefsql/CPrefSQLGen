#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Tuple and Preference generation
'''


import random

# Parameter values related to generation of table data
# Max value for attributes
MAX_VALUE = 32
# Min value for attributes
MIN_VALUE = 0

# Preference rules format
RULE_STRING = 'IF (0 <= a1 < {n0}) AND ({n1} <= a2 < {n2}) THEN ({x} <= a3 < {y}) BETTER ({y} <= a3 < {z}) {i}'
# Query
QUERY = '''SELECT {t} * FROM r
ACCORDING TO PREFERENCES
{p};'''


def gen_records(tup_number, att_number):
    '''
    Generate records
    '''
    # List of records
    rec_list = []
    # Loop to count the tuples number
    for _ in range(tup_number):
        # new tuple record
        new_record = {}
        # List of attributes
        att_list = ['a' + str(number + 1) for number in range(att_number)]
        # Generate each attribute value
        for att in att_list:
            new_record[att] = random.randint(0, MAX_VALUE)
        # Append record into list of records
        rec_list.append(new_record)
    # Return built record list
    return rec_list


def gen_rule(rule_dict):
    '''
    Convert rule dictionary into rule in string format
    '''
    return RULE_STRING.format(n0=rule_dict['n0'],
                              n1=rule_dict['n1'],
                              n2=rule_dict['n2'],
                              x=rule_dict['x'],
                              y=rule_dict['y'],
                              z=rule_dict['z'],
                              i=rule_dict['i'])


def gen_rules(n_rules, level, ind):
    '''
    Generate preference rules
    '''
    # Maximum value
    max_value = MAX_VALUE // 1

    # Initial condition
    n0 = 2

    # Number of intervals for first conditional
    condition_interval_number = n_rules // level

    # Length of Interval
    cond_block_length = max_value // condition_interval_number

    # Prefered interval block
    pref_block_length = max_value // (1 + level)

    # Preference level
    current_level = 0
    # Values for attributes of rule condition
    cond1 = 0

    # Indifferent attributes
    indiff_list = []
    # Build list of indifferent attributes
    indiff_str = ''
    # First indiferent attribute
    ind_start = 4

    # Preference value
    pref_value = 0

    if ind > 0:
        # Indifferent attributes start in A4
        for att_cont in range(ind):
            indiff_list.append('a' + str(att_cont + ind_start))
        indiff_str = '[' + ', '.join(indiff_list) + ']'

    # Build rules list
    rules_list = []
    for _ in range(n_rules):
        rule_dict = {}

        rule_dict['n0'] = n0
        rule_dict['n1'] = cond1 * cond_block_length
        rule_dict['n2'] = (cond1 + 1) * cond_block_length

        rule_dict['x'] = pref_value * pref_block_length
        rule_dict['y'] = (pref_value + 1) * pref_block_length
        rule_dict['z'] = (pref_value + 2) * pref_block_length

        pref_value += 1
        current_level += 1

        # Check if maximum level have been reached
        if current_level == level:
            current_level = 0
            pref_value = 0
            cond1 += 1

        rule_dict['i'] = indiff_str

        rules_list.append(gen_rule(rule_dict))
    return rules_list


def gen_query(n_rules, level, ind, top):
    '''
    Generate a preference query
    '''
    rules = gen_rules(n_rules, level, ind)
    topk = ''
    if top > -1:
        topk = 'TOP('+str(top)+')'

    prefs = '\nAND\n'.join(rules)
    complete_query = QUERY.format(t=topk,
                                  p=prefs)
    return complete_query
