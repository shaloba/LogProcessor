"""
author: Shlomy Balulu
date: 2017-02-20
Intro:
    In the code below we have two main classes:
        1. FileHandler- it is a file listener, when file created/ modify it trigger the LogProcessor
                        * on large scale prefer to use something like "FileBeat"
        2. LogProcessor- gets the log file path and retrieve its last lines by saving
                         the pointer in memory

    In order to sync the logs properly- I set "sync time" (can be after several hours/ each day etc)
    for the demonstration I set it to a few seconds
    when the last cache time elapsed the processor will sort the lost ascending and write it to the main log
    the precision of this script depends on the logs frequency, (for example- if the logs arriving in high frequency )
    we will prefer to increase the cache time)

    Usage:
    1. run the log_processor.py
       this script gets one parameter- logs directory path
       example: "python log_processor.py /var/log/nginx"
    2. create or modify the logs in the above path
    """

import sys
import os
import time
import re
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


LOG_SYNC_TIME = 10#in seconds


class LogProcessor():

    def __init__(self):
        self.output_log = os.path.join(os.getcwd(), 'processed_output.log')
        self.input_logs = {}
        self.cache = []
        self.last_sync_time = datetime.utcnow()

    def remove_input_log(self, file_path):
        """
        on file deletion it will remove the log from the input logs list
        :param file_path: the log path
        :return:
        """
        if self.input_logs.get(file_path):
            del self.input_logs[file_path]

    def should_sync(self):
        """
        check if
        :return:
        """
        if datetime.utcnow() > self.last_sync_time + timedelta(seconds=LOG_SYNC_TIME):
            return True
        return False

    def process_log(self, file_path):
        """
        Class main function, extract the log last lines and in case the cache time elapsed- write it to the
        main log
        :param file_path: the log path
        :return:
        """
        lines = self.get_last_lines(file_path)
        for line in lines:
            self.cache.append(line)

        sync = self.should_sync()
        if sync is True:
            cache = self.swap_cache()
            sorted_cache = self.sort_list(cache)
            self.sync_output_log(sorted_cache)

    def swap_cache(self):
        tmp_cache = self.cache
        self.cache = []
        return tmp_cache


    def sort_list(self, unsorted_list):
        """
        sort the cache list ascending
        :return:
        """
        pattern = '<(.*)?>'
        return sorted(unsorted_list, key=lambda c: float(re.findall(pattern, c)[0]), reverse=True)

    def sync_output_log(self, cache):
        """
        pop items from the cache list and write it to the main log
        :return:
        """
        try:
            fo = open(self.output_log, 'a+')
            for i in range(0, len(cache)):
                line = cache.pop()
                fo.write(line)
            fo.close()
            self.last_sync_time = datetime.utcnow()
        except Exception as err:
            print 'Error occured, [err] ' + str(err)

    def get_last_lines(self, file_path):
        """
        extract the log last line, check if the log has been reading before- if so it will retrive its
        last position
        :param file_path: the log path
        :return: list of strings (the log lines)
        """
        if self.input_logs.get(file_path):
            file_pointer = self.input_logs[file_path]
        else:
            file_pointer = 0
        try:
            with open(file_path, 'r') as fo:
                fo.seek(file_pointer)
                lines = fo.readlines()
                self.input_logs[file_path] = fo.tell()

        except Exception as err:
            print 'Error occured, [err] ' + str(err)
            lines = []
        return lines

class FileHandler(FileSystemEventHandler):

    def __init__(self):
        self.log_parser = LogProcessor()

    def process(self, event):
        """
        Main FileHandler function, triggers the log processor function
        :param event: the event
        :return:
        """
        if os.path.isfile(event.src_path):
            self.log_parser.process_log(event.src_path)

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)

    def on_deleted(self, event):
        self.log_parser.remove_input_log(event.src_path)


def run_consumer(path):
    observer = Observer()
    observer.schedule(FileHandler(), path, recursive=False)
    observer.start()
    observer.join()
    # the following lines is for the ability to exit by raising KeyboardInterrupt error
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

if __name__ == "__main__":
    logs_path = sys.argv[1] if len(sys.argv) > 1 else '.'
    if not os.path.isdir(logs_path):
        os.mkdir(logs_path)
    run_consumer(logs_path)
