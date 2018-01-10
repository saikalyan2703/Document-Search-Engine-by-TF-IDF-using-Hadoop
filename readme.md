							Instructions:

•MapReduce jobs were executed in Cloudera. A folder named wordcount is created in /user/cloudera and inside it input folder is created. All the canterbury files are moved from local file system to the input folder in Hadoop file system.

•To run DocWordCount job it must be provided with two arguments (input path which has input text files and output path).

For example, arguments and command must be like:
Arguments: /user/cloudera/wordcount/input /user/cloudera/wordcount/DocWordCountOutput
Command: Hadoop jar <Jar name> DocWordCount <arguments>

•To run TermFrequency job it must be provided with two arguments (input path which has input text files and output path).

For example, arguments and command must be like:
Arguments: /user/cloudera/wordcount/input /user/cloudera/wordcount/TermFrequencyOutput
Command: Hadoop jar <Jar name> TermFrequency <arguments>

•To run TFIDF job it must be provided with two arguments (input path which has input text files, output path for TermFrequency job and output path for TFIDF job). IntermediateOutput folder will be created in default Hadoop path. It must be deleted if exists to run TFIDF job.

For example, arguments and command must be like:
Arguments: /user/cloudera/wordcount/input /user/cloudera/wordcount/TFIDFOutput
Command: Hadoop jar <Jar name> TFIDF <arguments>

•We need to run TFIDF job before running Search. To run Search job, it must be provided with three arguments (Path which has the TFIDF output, output path for Search job and the input query given by the user). Query must be in quotations.

For example, arguments and command must be like:
Arguments: /user/cloudera/wordcount/TFIDFOutput /user/cloudera/wordcount/SearchOutput “query”
Command: Hadoop jar <Jar name> Search <arguments>

•We need to run Search job before running Rank. To run Rank job, it must be provided with two arguments (Path which has the Search output, output path for Rank job).

For example, arguments and command must be like:
Arguments: /user/cloudera/wordcount/SearchOutput /user/cloudera/wordcount/RankOutput
Command: Hadoop jar <Jar name> Rank <arguments>
