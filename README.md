# Parquet Writer

Valid command line Options:
--help <help>
-h <broker>
-b <broker>
-p <port>
-t <topic>
-q <QOS>
-v <verbose>
-d logging debug
-n <Client ID or Name>
-u Username
-P Password
-l <log directory default= tlogs>

Example:
You will always need to specify the broker name or IP address and the topics to log

Note: you may not need to use the python prefix or may
need to use python3 mqtt-topic-logger.py (Linux)

Specify broker and one topic

    python main.py -h 192.168.1.157 -t sensors/#

Specify broker and multiple topics

    python main.py -h 192.168.1.157 -t sensors/# -t  home/#

Specify the client name used by the logger : data-logger

    python main.py -h 192.168.1.157 -t sensors/# -n data-logger

Specify the log directory : mylogs

    python main.py -h 192.168.1.157 -t sensors/# -l mylogs
