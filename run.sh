#!/usr/bin/env bash
hadoop fs -rm -r out >/dev/null 2>&1
hadoop jar jar/UrlCount.jar ru.mipt.UrlCount /data/socnet_urls out >/dev/null 2>&1
hadoop fs -cat out/part-r-00000 | head -40
