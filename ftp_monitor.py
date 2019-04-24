import datetime
import logging
import logging.config
import logging.handlers
import math
import multiprocessing as mp
import re
import shutil
import time
import traceback
import types
import uuid
from collections import OrderedDict

from retry import retry

from basic_settings import *

new_folder_key = 'New FTP Folder'
target_folder_key = 'Target Folder'
window_pid_key = 'Windows PID'
transfer_status_key = 'Transfer Status'
checking_status = 'Checking'
starting_status = 'Starting'
transferred_status = 'Transferred'
transferring_status = 'Transferring'
copying_status = 'Copying'
copied_status = 'Copied'
failure_status = 'Failure'

if not os.path.isdir(main_log_folder):
    os.makedirs(main_log_folder)


def get_file_time(file_path):
    try:
        import time
        from datetime import datetime

        modified_time = datetime.strptime(time.ctime(os.path.getmtime(file_path)), '%a %b %d %H:%M:%S %Y')
        created_time = datetime.strptime(time.ctime(os.path.getctime(file_path)), '%a %b %d %H:%M:%S %Y')
        return modified_time, created_time
    except Exception as e:
        traceback.print_exc()
        raise e


@retry(tries=5, delay=15)
def get_folder_info(start_path, logger=None):
    try:
        if logger is not None:
            logger.info('Get info of {}'.format(start_path))
        else:
            print('Get info of {}'.format(start_path))

        if os.path.isdir(start_path):
            total_size = 0
            total_number = 0
            latest_time = None
            for dirpath, dirnames, filenames in os.walk(start_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)
                    total_number += 1

                    file_time = get_file_time(fp)[0]
                    if latest_time is None:
                        latest_time = file_time
                    else:
                        if latest_time < file_time:
                            latest_time = file_time

            if logger is not None:
                logger.info('Get folder info:\n{}\nTotal Size: {}\nTotal Number: {}\nLatest Time: {}'.format(hyper_link_file_path(start_path),
                                                                                                             convert_size(total_size),
                                                                                                             total_number,
                                                                                                             latest_time))
            return total_size, total_number, latest_time

        else:
            raise Exception('{} does not exist'.format(start_path))

    except Exception as e:
        if logger is not None:
            logger.exception(e)
        else:
            traceback.print_exc()
        raise e


def create_logger(log_folder, logger_name):
    try:
        def log_newline(self, how_many_lines=1):
            # Switch handler, output a blank line
            self.removeHandler(self.streamHandler)
            self.addHandler(self.blank_streamHandler)
            self.removeHandler(self.fileHandler)
            self.addHandler(self.blank_fileHandler)
            for i in range(how_many_lines):
                self.info('')
            # Switch back
            self.removeHandler(self.blank_streamHandler)
            self.addHandler(self.streamHandler)
            self.removeHandler(self.blank_fileHandler)
            self.addHandler(self.fileHandler)

        FORMAT = '%(asctime)s- %(name)s - %(message)s'
        DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT)

        log_file_path = log_folder + "\\" + "{0}_{1}.txt".format(logger_name, time.strftime("%Y_%m_%d_%H_%M_%S"))
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)

        fileHandler = logging.FileHandler(filename=log_file_path)
        fileHandler.setLevel(logging.INFO)
        fileHandler.setFormatter(formatter)

        blank_streamHandler = logging.StreamHandler()
        blank_streamHandler.setLevel(logging.DEBUG)
        blank_streamHandler.setFormatter(logging.Formatter(fmt=''))

        blank_fileHandler = logging.FileHandler(filename=log_file_path)
        blank_fileHandler.setLevel(logging.INFO)
        blank_fileHandler.setFormatter(logging.Formatter(fmt=''))

        logger.streamHandler = streamHandler
        logger.fileHandler = fileHandler
        logger.blank_streamHandler = blank_streamHandler
        logger.blank_fileHandler = blank_fileHandler
        logger.newLine = types.MethodType(log_newline, logger)

        logger.addHandler(streamHandler)
        logger.addHandler(fileHandler)

        return logger, log_file_path
    except Exception as e:
        raise


def create_time_string(time_seconds):
    try:
        time_minutes = math.ceil(time_seconds / 60.0)

        if time_minutes < 60:
            time_string = '{0} minutes'.format(time_minutes)
        else:
            time_hours = round(time_minutes / 60.0, 1)
            time_string = '{0} hours'.format(time_hours)

        return time_string
    except:
        traceback.print_exc()
        raise


def convert_size(size):
    size_kb = size / 1000.0
    if size_kb == 0:
        return '0 B'
    size_name = ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_kb, 1024)))
    p = math.pow(1024, i)
    s = round(size_kb / p, 2)
    return '%s %s' % (s, size_name[i])


def hyper_link_file_path(file_path):
    try:
        filePathLink = u'<a href="{0}">{0}</a>'.format(file_path)
        return filePathLink
    except Exception as e:
        raise e


class FolderTransferWorker(object):
    """Class for transferring new folder"""

    def __init__(self, new_folder, master_monitor_dict):
        self.tracking_uuid = str(uuid.uuid1())
        self.new_folder = new_folder
        self.new_folder_name = os.path.basename(self.new_folder)

        self.target_folder = os.path.join(target_root_folder, datetime.date.today().strftime('%Y-%m-%d'),
                                          self.new_folder_name)

        self.archive_folder = os.path.join(archive_root_folder, datetime.date.today().strftime('%Y-%m-%d'),
                                           self.new_folder_name)

        self.check_break_minutes = uploading_checking_break_minutes
        self.logger, self.log_file = create_logger(main_log_folder, self.new_folder_name.replace(' ', '_'))
        self.time_out_hours = uploading_checking_time_out_hours
        self.timed_out = False
        self.finished = False
        self.master_monitor_dict = master_monitor_dict
        self.tracking_ordered_dict = OrderedDict([
            (window_pid_key, os.getpid()),
            (new_folder_key, self.new_folder),
            (target_folder_key, self.target_folder),
            (transfer_status_key, starting_status)
        ]
        )
        self.master_monitor_dict[self.tracking_uuid] = self.tracking_ordered_dict

    def update_transfer_status(self, status, target_folder=None):
        self.tracking_ordered_dict[transfer_status_key] = status
        if target_folder is not None:
            self.tracking_ordered_dict[target_folder_key] = target_folder

        self.master_monitor_dict[self.tracking_uuid] = self.tracking_ordered_dict

    def check_if_completed(self):
        self.logger.info('Checking if {0} is done with uploading'.format(self.new_folder))

        self.update_transfer_status(checking_status)

        total_check_seconds = 0
        time_out_seconds = self.time_out_hours * 3600

        total_size_old, total_number_old, latest_time_old = get_folder_info(self.new_folder, self.logger)

        while not self.finished and total_check_seconds < time_out_seconds:

            self.logger.info('Will check again in {0} minutes'.format(self.check_break_minutes))
            time.sleep(self.check_break_minutes * 60)
            total_size_new, total_number_new, latest_time_new = get_folder_info(self.new_folder, self.logger)

            if total_number_old != total_number_new \
                    or total_size_old != total_size_new \
                    or latest_time_old != latest_time_new:

                if total_number_new == 0 and total_size_new == 0 and latest_time_new is None:
                    raise Exception('The content of {} has been deleted !'.format(self.new_folder))

                total_check_seconds += self.check_break_minutes * 60
                self.logger.info('Not Finished')
                total_size_old, total_number_old, latest_time_old \
                    = total_size_new, total_number_new, latest_time_new
            else:
                self.finished = True

        if not self.finished:
            self.logger.info('Not Finished')
            self.logger.info('Time out {0}'.format(create_time_string(total_check_seconds)))
            failure_status_compiled = '{}:Time out in {} hours'.format(failure_status, self.time_out_hours)
            self.update_transfer_status(failure_status_compiled)
        else:
            self.logger.info('Uploading is done')
            self.update_transfer_status(transferred_status)

    def start_working(self):
        try:
            self.logger.info('Starting working on {}'.format(self.new_folder_name))

            self.check_if_completed()

            if self.finished:

                if os.path.isdir(self.target_folder):
                    self.target_folder = os.path.join(os.path.dirname(self.target_folder),
                                                      '{}_{}'.format(os.path.basename(self.target_folder),
                                                                     datetime.datetime.now().strftime('%Y%m%d%H%M%S')))

                self.update_transfer_status(copying_status, target_folder=self.target_folder)

                self.logger.info('Starting copying {} to {}'.format(self.new_folder, self.target_folder))
                shutil.copytree(self.new_folder, self.target_folder)
                self.logger.info('Done with copying {}'.format(self.new_folder_name))

                self.update_transfer_status(copied_status)

                if os.path.isdir(self.archive_folder):
                    self.archive_folder = os.path.join(os.path.dirname(self.archive_folder),
                                                       '{}_{}'.format(os.path.basename(self.archive_folder),
                                                                      datetime.datetime.now().strftime('%Y%m%d%H%M%S')))

                self.logger.info('Starting archiving {} to {}'.format(self.new_folder, self.archive_folder))
                shutil.move(self.new_folder, self.archive_folder)
                self.logger.info('Done with archiving {}'.format(self.new_folder_name))

        except Exception as e:
            self.logger.exception(e)
            failure_status_compiled = '{}: {}'.format(failure_status, str(e))
            self.update_transfer_status(failure_status_compiled)


class FTPMonitor(object):
    """Class for monitoring ftp folder"""

    def __init__(self):
        self.ftp_folder = ftp_folder
        self.monitor_pool = mp.Pool(mp.cpu_count() - 1)
        self.master_monitor_dict = mp.Manager().dict()
        self.all_tracking_objects = []
        self.all_active_objects = []
        self.all_completed_objects = []
        self.all_processed_folders = []
        self.report_dict = {}
        self.main_program_break_minutes = main_program_break_minutes
        self.main_logger, self.main_log_file = create_logger(main_log_folder, self.__class__.__name__)

    def report_status(self):
        self.main_logger.newLine()
        self.main_logger.info('Reporting transferring status')
        all_status_dict = {tracking_id: self.master_monitor_dict[tracking_id] for tracking_id in
                           self.master_monitor_dict.keys()}

        checking_dict = {tracking_id: all_status_dict[tracking_id] for tracking_id in
                         all_status_dict.keys()
                         if re.search(checking_status,
                                      all_status_dict[tracking_id][transfer_status_key],
                                      re.IGNORECASE)}
        transferring_dict = {tracking_id: all_status_dict[tracking_id] for tracking_id in
                             all_status_dict.keys()
                             if re.search(transferring_status,
                                          all_status_dict[tracking_id][transfer_status_key],
                                          re.IGNORECASE)}
        transferred_dict = {tracking_id: all_status_dict[tracking_id] for tracking_id in
                            all_status_dict.keys()
                            if re.search(transferred_status,
                                         all_status_dict[tracking_id][transfer_status_key],
                                         re.IGNORECASE)}
        copied_dict = {tracking_id: all_status_dict[tracking_id] for tracking_id in
                       all_status_dict.keys()
                       if re.search(copied_status,
                                    all_status_dict[tracking_id][transfer_status_key],
                                    re.IGNORECASE)}
        copying_dict = {tracking_id: all_status_dict[tracking_id] for tracking_id in
                        all_status_dict.keys()
                        if re.search(copying_status,
                                     all_status_dict[tracking_id][transfer_status_key],
                                     re.IGNORECASE)}
        failure_dict = {tracking_id: all_status_dict[tracking_id] for tracking_id in
                        all_status_dict.keys()
                        if re.search(failure_status,
                                     all_status_dict[tracking_id][transfer_status_key],
                                     re.IGNORECASE)}

        self.report_dict = OrderedDict([
            (checking_status, checking_dict),
            (transferred_status, transferred_dict),
            (transferring_status, transferring_dict),
            (copying_status, copying_dict),
            (copied_status, copied_dict),
            (failure_status, failure_dict)
        ])

        count = 0
        for status in self.report_dict.keys():
            status_dict = self.report_dict[status]
            if len(status_dict) > 0:
                count += 1
                self.main_logger.info('{}. {}'.format(count, status))
                for tracking_index, tracking_id in enumerate(status_dict.keys()):
                    tracking_details = '({}). {}'.format(tracking_index + 1,
                                                         status_dict[tracking_id][target_folder_key])
                    if status == failure_status:
                        tracking_details += ' - {}'.format(status_dict[tracking_id][transfer_status_key])

                    self.main_logger.info(tracking_details)

    def run(self):
        keep_running = True

        today = datetime.date.today()
        today_ending_time = datetime.datetime(year=today.year, month=today.month, day=today.day,
                                              hour=daily_ending_hour)

        self.main_logger.info('Start working at {}'.format(time.strftime('%H:%M:%S')))
        self.main_logger.newLine()

        while keep_running:
            folder_lists = [os.path.join(self.ftp_folder, item)
                            for item in os.listdir(self.ftp_folder)
                            if os.path.isdir(os.path.join(self.ftp_folder, item))]
            new_folders_list = [item for item in folder_lists
                                if item not in self.all_processed_folders]

            if len(new_folders_list) == 0:
                self.main_logger.info('No new folders')
            else:
                self.main_logger.info('{} new folders found:\n{}'.format(len(new_folders_list),
                                                                         '\n'.join(new_folders_list)))

            self.main_logger.newLine()
            for new_folder in new_folders_list:
                new_folder_name = os.path.basename(new_folder)
                self.main_logger.info('Putting {} into pool'.format(new_folder_name))
                self.all_tracking_objects.append(self.monitor_pool.apply_async(process_new_folder,
                                                                               (new_folder,
                                                                                self.master_monitor_dict)))

                self.all_processed_folders.append(new_folder)

            time.sleep(10)
            self.report_status()
            self.main_logger.newLine()
            self.main_logger.info('Take {} minutes break'.format(self.main_program_break_minutes))
            time.sleep(self.main_program_break_minutes * 60)

            self.all_active_objects = [x for x in self.all_tracking_objects
                                       if x is not None and
                                       (not x.ready() or (x.ready() and x.get()[0] is None))]

            self.all_completed_objects = [x for x in self.all_tracking_objects
                                          if x is not None and
                                          x.ready() and x.get()[0] is not None]

            self.main_logger.newLine()
            self.main_logger.info('Currently {} active processing'.format(len(self.all_active_objects)))
            self.main_logger.info('Currently {} completed processing'.format(len(self.all_completed_objects)))

            if datetime.datetime.now() > today_ending_time:
                if len(self.all_active_objects) == 0:
                    self.main_logger.info('No active jobs now. Done with today')
                    keep_running = False

        self.monitor_pool.close()
        self.monitor_pool.join()


def process_new_folder(new_folder, master_monitor_dict):
    try:
        transfer_worker = FolderTransferWorker(new_folder, master_monitor_dict)
        transfer_worker.start_working()
        return 'success'
    except Exception as e:
        return 'failure: {}'.format(str(e))


if __name__ == '__main__':
    pass
