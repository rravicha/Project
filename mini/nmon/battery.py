import psutil
from datetime import datetime as dt

import time

while True:
    try:
        per=psutil.sensors_battery().percent
        cd=dt.today()
        time.sleep(60)
        
    except KeyboardInterrupt:
        pass
    finally:
        with open('track.bat','a') as fp:
            fp.writelines((f"{per},{cd}\n"))