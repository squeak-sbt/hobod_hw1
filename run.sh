#!/usr/bin/env bash
hadoop fs -rm -r out result
hadoop jar jar/UrlCount.jar ru.mipt.UrlCount /data/socnet_urls out
hadoop fs -cat out/part-r-00000 | head -40