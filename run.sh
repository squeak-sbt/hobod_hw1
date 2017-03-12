#!/usr/bin/env bash
hadoop fs -rm -r out result
hadoop jar jar/UrlCount.jar ru.mipt.UrlCount /data/socnet_urls out
hadoop jar jar/UrlCount.jar ru.mipt.UrlSort out result
hadoop fs -cat result/part-r-00000 | head -10