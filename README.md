# CPrefSQLGen
CPrefSQLGen is a dataset generator for evaluation of the operators of the CPrefSQLGen query language.
It's capable of generating tuples and queries, storing them on files.
Running experiments and summarizing the results in CSV files.

## Installation Instructions
To use the generator, the following are required:

- PostgreSQL must be installed and configured;
- PostgreSQL connection variables on file *cprefsql_postgresql.py* must be set to a valid user and database;
- CPrefSQL files must be installed somewhere in the system and that directory must be added to **PYTHONPATH** environment variable to allow the generator to have access to those libraries (avaliable here at https://github.com/cprefsql/cprefsql);
- The files of the generator should also be coppied to some directory in the system and also be added to **PYTHONPATH** environment variable.

Configuring the environment variable can be easily done with the following command on Linus:
```
    export PYTHONPATH="${PYTHONPATH}:/my/cprefsql/path"
```

## Description
There are two scripts: *cprefsql_postgresql.py* and *cprefsql_sqlite.py*, which handle CPrefSQL operations for the experiments both on PostgreSQL DBMS and SQLite with local files. Generator is operating by running the main file *generator.py*, which contains several variables that guide experiment execution. Each experiment varies varies specific attributes that affect the generation of the data or the execution of the queries.

The variables varied by the experiments are:
- ATT: Number of Attributes on tuples;
- TUP: Number of tuples in the database generated;
- RUL: Number of rules in the cp-theories generated;
- IND: Number of indifferent attributes on queries;
- LEV: Max preference level or maximal level on the cp-theories;
- TOP: Number of TOPK results.

To run the queries, three algorithms are available:
- Partition Algorithms (partition)
- BNL Algorithms (bnl)
- New Partition algorithms with new hierarchy model (maxpref)

The generator runs each experiment a given number of times (set on the script) and save the data on CSV files on directories it created for that purpose. Each query run, data on execution time and memory used are collected and saved for summarization later.

## Command Line Options
The generator has the following command line options:
```
usage: CPrefSQLGen [-h] [-g] [-r] [-s]

optional arguments:
  -h, --help       Show help message
  -g, --gen        Generate files
  -r, --run        Run experiments
  -s, --summarize  Summarize results
```
- Option `--gen` create directories required inside folder `experiments` on local directory to store the files and generate the random tuples and queries files with CPrefSQL cp-theories;
- Option `--run` run experiments, each experiment saves its results to files, allowing them to be interrupted and restarted from the last experiment concluded;
- Option --summarize compute statistical useful values and store them in CSV files that can be exported to tools like LaTeX and Excel.
Please also check CPrefSQL repository \([https://cprefsql.github.io/cprefsql/](https://cprefsql.github.io/cprefsql/)\)

