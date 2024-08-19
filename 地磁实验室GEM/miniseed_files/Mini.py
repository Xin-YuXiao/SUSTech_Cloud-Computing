from obspy.core import read
import os
import numpy as np
import sys

#pyPath=os.getcwd()
#filePath=pyPath+'/accel'
filePath=sys.argv[1]
mseedFileNames=os.listdir(filePath)
count=0
for mseedFileName in mseedFileNames:
    if mseedFileName[-5:]!="mseed":
        continue
    st=read(filePath+'/'+mseedFileName)
    traceNum=len(st)
    if traceNum==1:
        tr=st[0]
        st.write(filePath+'/'+mseedFileName+'_TSPAIR',format='TSPAIR')
        count=count+1
        print('%04d-------->%s' %(count,mseedFileName))
    else:
        print('ERROR:the number of the trace is not one ??? %s' %(mseedFileName))

print("Finished!")