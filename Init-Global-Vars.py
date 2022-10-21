# Databricks notebook source
INPUT_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
LOCAL_STORAGE_SPARK_API_FORMAT = 'dbfs:/FileStore/WikipediaLab/input_jsons'
LOCAL_STORAGE_FILES_API_FORMAT = '/dbfs/FileStore/WikipediaLab/input_jsons'

ROWS_PER_FILE = 30
FILES_LIMIT = 10
