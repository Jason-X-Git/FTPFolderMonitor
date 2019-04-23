import os

# 5 minutes for small folder, and 20 minutes for large folder are recommended.
uploading_checking_break_minutes = 20
# Set a timeout for monitoring a folder which is not completed in uploading within 2 hours
uploading_checking_time_out_hours = 2
# Take a break for the checking loop, 5 minutes recommended.
main_program_break_minutes = 5
# The program stops at this time everyday if you run it in task schedule
daily_ending_hour = 21

# The folder will be monitored
ftp_folder = r'C:\ftp_test\source'

# The folder where the source files will be moved into.
target_root_folder = r'c:\ftp_test\target'

# The folder where the source files will be archived into.
archive_root_folder = r'c:\ftp_test\archive'

# The folder is used to save all logs files.
main_log_folder = r'c:\ftp_test\logs'

for item in [ftp_folder, target_root_folder, archive_root_folder, main_log_folder]:
    if not os.path.isdir(item):
        raise Exception('{} does not exist'.format(item))
