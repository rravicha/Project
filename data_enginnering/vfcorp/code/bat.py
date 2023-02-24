import os;os.system('clear')
# # python script showing battery details
# import psutil
# import time
# # function returning time in hh:mm:ss
def convertTime(seconds):
	minutes, seconds = divmod(seconds, 60)
	hours, minutes = divmod(minutes, 60)
	return "%d:%02d:%02d" % (hours, minutes, seconds)

# # returns a tuple


# while 1:
#     battery = psutil.sensors_battery()
#     # print("Battery percentage : ", battery.percent)
#     # print("Power plugged in : ", battery.power_plugged)
#     # print("Battery left : ", convertTime(battery.secsleft))
#     print(f"{battery.percent} | {battery.power_plugged} | {convertTime(battery.secsleft)}")
#     time.sleep(5)
#     print('_'*40)

import psutil
import time
fun=['boot_time', 'collections', 'contextlib', 'cpu_count', 'cpu_freq', 'cpu_percent', 'cpu_stats', 'cpu_times', \
    'cpu_times_percent', 'datetime', 'disk_io_counters', 'disk_partitions', 'disk_usage', 'errno', 'functools', \
        'long', 'net_connections', 'net_if_addrs', 'net_if_stats', 'net_io_counters', 'os', 'pid_exists', 'pids', \
            'process_iter', 'pwd', 'sensors_battery', 'sensors_fans', 'sensors_temperatures', 'signal', 'subprocess',\
                 'swap_memory', 'sys', 'test', 'threading', 'time', 'users', 'version_info', 'virtual_memory', \
                    'wait_procs']
# print(f"""
# psutil.sensors_battery:{psutil.sensors_battery()}
# psutil.sensors_fans:{psutil.sensors_fans()}
# psutil.sensors_battery:{psutil.sensors_battery()}
# psutil.sensors_temperatures:{psutil.sensors_temperatures()}
# """)
'''
psutil.sensors_battery:sbattery(percent=17.993630573248407, secsleft=<BatteryTime.POWER_TIME_UNLIMITED: -2>, power_plugged=True)
psutil.sensors_fans:{'dell_smm': [sfan(label='Processor Fan', current=2729)]}
psutil.sensors_battery:sbattery(percent=17.993630573248407, secsleft=<BatteryTime.POWER_TIME_UNLIMITED: -2>, power_plugged=True)
psutil.sensors_temperatures:{'acpitz': [shwtemp(label='', current=39.0, high=98.0, critical=98.0)], 'amdgpu': [shwtemp(label='edge', current=39.0, high=None, critical=None)], 'k10temp': [shwtemp(label='', current=39.625, high=70.0, critical=100.0)], 'dell_smm': [shwtemp(label='CPU', current=39.0, high=None, critical=None), shwtemp(label='Ambient', current=48.0, high=None, critical=None), shwtemp(label='GPU', current=16.0, high=None, critical=None)]}
'''
bat=psutil.sensors_battery()
fan=psutil.sensors_fans()
tmp=psutil.sensors_temperatures()
def info():
    print(f"""
___________________________________________________________________________________________________________________________________
BATTERY:
-------
PERCENT : {bat.percent}% | PLUGGED : {bat.power_plugged}| POWER LEFT : {convertTime(bat.secsleft)}

CPU FAN STATS:
--------------
SPEED : {fan} 
TEMPERATURE:
------------
{tmp['acpitz']}
{tmp['amdgpu']}
{tmp['dell_smm']}
{tmp['k10temp'][0]}
___________________________________________________________________________________________________________________________________
""")
def ref():
    print(f"""
___________________________________________________________________________________________________________________________________
BATTERY:
-------
PERCENT : ////////////////// / PLUGGED : ///////// POWER LEFT : ////////.

CPU FAN STATS:
--------------
SPEED : ////////////////
TEMPERATURE:
------------
////////////////
////////////////
////////////////
////////////////
////////////////
___________________________________________________________________________________________________________________________________
""")
def init():
    print(f"""
___________________________________________________________________________________________________________________________________
BATTERY:
-------
PERCENT : \\\\\\\\\\\\\\\\\\ \ PLUGGED : \\\\\\\\\ POWER LEFT : \\\\\\\\.

CPU FAN STATS:
--------------
SPEED : \\\\\\\\\\\\\\\\
TEMPERATURE:
------------
\\\\\\\\\\\\\\\\
\\\\\\\\\\\\\\\\
\\\\\\\\\\\\\\\\
\\\\\\\\\\\\\\\\
\\\\\\\\\\\\\\\\
___________________________________________________________________________________________________________________________________
""")
while 1:
    info()
    time.sleep(1)
    os.system('clear')
    ref()
    time.sleep(0.2)
    os.system('clear') 
    init()
    time.sleep(0.2) 
    os.system('clear')