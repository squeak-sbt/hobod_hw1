#!/usr/bin/env bash
hadoop fs -rm -r out > /dev/null
hadoop jar jar/UrlCount.jar ru.mipt.UrlCount /data/socnet_urls out > /dev/null
hadoop fs -cat out/part-r-00000 | head -40