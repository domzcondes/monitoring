import csv
import datetime
import psutil
import shutil
import os
import string

# Output CSV file path
output_file = r'update this - usage.csv'

# Get current date and time
now = datetime.datetime.now()
dt = now.strftime('%Y.%m.%d %H:%M:%S')

# Prepare data to write
rows = []

# ----- CPU Usage -----
cpu_usage = psutil.cpu_percent(interval=1)
print(f"CPU Usage: {cpu_usage}%")
rows.append([dt, 'CPU Usage', cpu_usage, 100])  # 100% as threshold

# ----- RAM Usage -----
virtual_mem = psutil.virtual_memory()
in_use_memory = virtual_mem.used
total_memory = virtual_mem.total
print(f"Memory In Use: {in_use_memory} / {total_memory}")
rows.append([dt, 'Memory Usage', in_use_memory, total_memory])

# ----- Drive Usage (C to Z) -----
for drive_letter in string.ascii_uppercase:
    drive_path = f'{drive_letter}:\\'
    if os.path.exists(drive_path):
        try:
            usage = shutil.disk_usage(drive_path)
            free_space = usage.free
            total_space = usage.total
            print(f"{drive_letter}: Free Space: {free_space} / {total_space}")
            rows.append([dt, f'{drive_letter}: Free Space', free_space, total_space])
        except Exception as e:
            print(f"Error reading {drive_path}: {e}")

# ----- Write to CSV -----
header = ['Timestamp', 'Metric', 'Value', 'Threshold']
write_header = not os.path.exists(output_file)

with open(output_file, 'a', newline='') as f:
    writer = csv.writer(f, delimiter='|')
    if write_header:
        writer.writerow(header)
    writer.writerows(rows)

print("Monitoring data written to CSV.")
