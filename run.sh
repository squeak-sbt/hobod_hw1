#!/usr/bin/env bash
hadoop fs -rm -r out
hadoop jar jar/UrlCount.jar ru.mipt.UrlCount /data/socnet_urls out
