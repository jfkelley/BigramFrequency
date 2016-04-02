#Calculating bigram frequency in documents

Given a directory containing text documents (one per file), calculates the global frequency of bigrams.

Expected arguments:

```
--input <path>
--output <path>
--adjacent (optional)
--spark (optional)
```

If `--adjacent` is specified, only pairs of words which appear next to each other will be counted. Otherwise, all pairs within a document will be counted.

By default, runs in a single thread using simple Scala collections. But if `--spark` is specified, will run as a spark application.

Reads input from all files in the directory specified by `--input <path>`. If in "local" mode, writes output to a single file (specified by `--output <path>`). If in "spark" mode, writes multiple output files into an output directory (specified by `--output <path>`).

The "sample_input" directory contains the first 500 lines of Shakespeare's sonnets, one line per file

Example invocations:

```
sbt "run --input sample_input --output output.txt"
sbt "run --input sample_input --output output.txt --adjacent"
sbt "run --input sample_input --output output_dir --spark"
sbt "run --input sample_input --output output_dir --spark" --adjacent
```