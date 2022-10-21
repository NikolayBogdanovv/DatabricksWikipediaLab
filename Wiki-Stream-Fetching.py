# Databricks notebook source
# MAGIC %run ./Init-Global-Vars

# COMMAND ----------

import json
from datetime import datetime
from dataclasses import dataclass

from sseclient import SSEClient as EventSource


# COMMAND ----------

@dataclass
class WikiChangeObject:
    edited_ts: str
    user: str
    title: str
    is_bot: str
    domain: str
    
    def to_json(self):
        return f'{{"edited_ts":"{self.edited_ts}","user":"{self.user}","title":"{self.title}","is_bot":"{self.is_bot}","domain":"{self.domain}"}}\n'

# COMMAND ----------

files_counter = 0
changes = []
current_dt = datetime.now()
filename_prefix = f'{current_dt.year}_{current_dt.month}_{current_dt.day}_{current_dt.hour}_{current_dt.minute}_{current_dt.second}'
for event in EventSource(INPUT_STREAM_URL):
    if event.event == 'message':
        try:
            change = json.loads(event.data)
        except ValueError:
            continue
        current_object = WikiChangeObject(
            edited_ts = change['meta']['dt'],
            user = change['user'],
            title = change['title'],
            is_bot = change['bot'],
            domain = change['meta']['domain']
        )
        #
        if current_object.domain == 'www.wikidata.org':
            continue
        #
        changes.append(current_object)
        if len(changes) == ROWS_PER_FILE:
            filename = f'{filename_prefix}_{files_counter+1}.json'
            with open(f'{LOCAL_STORAGE_FILES_API_FORMAT}/{filename}', 'w') as outfile:
                for current_change in changes:
                    outfile.write(current_change.to_json())
            print(f'{filename} added')
            files_counter += 1
            changes = []
            if FILES_LIMIT and files_counter == FILES_LIMIT:          
                break
