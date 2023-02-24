import glob
import os
import subprocess
os.chdir("/home/susi/github/Project/hps/container")
i=0
for file in glob.glob('*.dat'):
    try:
        print(f"filename processing - {file}")
    except e:
        print(str(e))
    