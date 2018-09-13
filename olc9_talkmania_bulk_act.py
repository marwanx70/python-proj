#!/usr/local/bin/python
import multiprocessing
import sys
import datetime
import os
import os.path
import thread
import threading
import socket
import ConfigParser
import re
import time
import itertools
import fileinput
import glob

sys.path.append(os.path.expanduser('~/pbin/lib'))
import cx_Oracle
import subprocess
from libDiameter import *
from itertools import islice
from shutil import copyfile
#from glob import glob
from  string import  Formatter
starts=0
ends=0
threshold=0
sleeps=0
site=sys.argv[1]
start_Peak_Hours=0
end_Peak_Hours=0
threadLock=threading.Lock()
localtime=time.localtime()
proc_count=1
home_path=os.path.expandvars('$HOME')
olc_path="%s/var/tks/projs/olc"%home_path
#file_path=home_path+'/TalkMania/Work/'+site+'/*.ready'
file_path="%s/TalkMania/Work/%s/*.ready"%(olc_path,site)
file_path_jkt="%s/TalkMania/Work2/%s/*.ready"%(olc_path,site)
decode_path="%s/TalkMania/Work/%s"%(olc_path,site)
temp_decode=os.path.join(decode_path,'Temp_decode.txt')
#full_path=os.path.join(file_path,'TMBulk.txt')
#app_path=home_path+'/pbin/app'
app_path="%s/pbin/app"%home_path
#log_path=home_path+'/TalkMania/Log'
log_path=('%s/TalkMania/Log/%s'%(olc_path,site))
#log_stat=home_path+'/TalkMania/Log/'
stop_path=olc_path+'/TalkMania/Stop/'+site+'.stop'
control_threshold=olc_path+'/TalkMania/Stop/'+site+'/Control_Threshold.txt'
log_stat=("%s/TalkMania/Log/"%olc_path)
timestring=time.strftime("%Y%m%d%H%M%S", localtime)
succ_Filename=os.path.join(log_path,'TMBulk_%s_%s_SUCCESS.log'%(timestring,site))
fail_Filename=os.path.join(log_path,'TMBulk_%s_%s_FAIL.log'%(timestring,site))
#successLog=open(succ_Filename,'a+')
#failLog=open(fail_Filename,'a+')
control_stat_file=os.path.join(app_path,'olc9_talkmania_stat.ctl')
xml_file=os.path.join(app_path,'dictDiameter.xml')
LoadDictionary(xml_file)
#start of routine send_ccr_auth_messages()
def worker(host,port,list_message):
#def worker(host,port,starts,ends,list_message):
       global start_Peak_Hours
       global end_Peak_Hours
       counter=0
       received_List=[]
       port=int(port)
       successLog=open(succ_Filename,'a+')
       failLog=open(fail_Filename,'a+')
       ctlThr=open(control_threshold,'a+')
       val_range=len(list_message)
       for i in  xrange(val_range):
	       counter+=1
	       msisdn=list_message[i][1]
	       tm_activation_id=list_message[i][2]
	       session_id=list_message[i][3]
	       currtime=time.strftime("%H:%M", localtime)
	       split_Time=currtime.split(':')
	       hours=int(split_Time[0])
	       minutes=int(split_Time[1])
	       sleep_Time=(end_Peak_Hours-hours-1)*3600 + (60-minutes)*60
	       if (hours >=start_Peak_Hours and hours <end_Peak_Hours):
	       	   print('Start Sleep Time : ',currtime)
	       	   time.sleep(sleep_Time)
	       	   print ('End Sleep Time : ',time.strftime("%H:%M", localtime))
               try:
                   Conn=Connect(host,port)
	           Conn.send(list_message[i][0].decode('hex'))
	           received = Conn.recv(1024)
		   time.sleep(0.4)
	           msg=received.encode('hex')
		   time.sleep(0.4)
	           H=HDRItem()
	           stripHdr(H,msg)
	           avps=splitMsgAVPs(H.msg)
                   msccret=findAVP("Multiple-Services-Credit-Control",avps)
	           result_code=findAVP("Result-Code",msccret)
	           starttime=time.localtime()
	           event_Time=time.strftime("%Y%m%d%H%M%S", starttime)
	           sessionid=session_id.replace('\n', '')
                   log=msisdn+"|"+tm_activation_id+"|"+sessionid+"|"+site+"|"+filename+"|"+event_Time+"|"+str(result_code)+"\n"

	           if(result_code==2001):
		      successLog.write(log)
		      successLog.flush()
	           else:
	               failLog.write(log)
		       failLog.flush()
                   ctlThr.write('x\n')
		   ctlThr.flush()
		   time.sleep(0.5)

	       except Exception,e:
	           ctlThr.write('x\n')
		   ctlThr.flush()
		   starttime=time.localtime()
       	           event_Time=time.strftime("%Y%m%d%H%M%S", starttime)
	           sessionid=session_id.replace('\n', '')
	           failLog.write(msisdn+"|"+tm_activation_id+"|"+sessionid+"|"+site+"|"+filename+"|"+event_Time+"|3004\n")
       successLog.close()
       failLog.close()
           

def worker2(inp_file,start2,end2):
	startval=0
        endval=0
	tempval=0

	m_list=[]
	currtime=time.strftime("%H:%M", localtime)
	with open(inp_file,'r') as file:
             for data in itertools.islice(file,start2,end2):
		 col=data.split('|')
                 msisdn=col[0]
		 tm_activation_id=col[1]
	         session_id=col[2]
		 data_list=[]
                 message=createCCR(msisdn,tm_activation_id,session_id)
                 data_list.append(message)
		 data_list.append(msisdn)
		 data_list.append(tm_activation_id)
		 data_list.append(session_id)
		 m_list.append(data_list)
        currtime=time.strftime("%H:%M", localtime)
	remaining_list=len(m_list)
	jobs=[]
        mod_seq=len(m_list)%len(host)
        if (mod_seq>0):
	     starts=0 
	     rangeval=(len(m_list)/len(host))+1
	     ends=rangeval
	     for loop in xrange(rangeval):
	          if loop==rangeval-1:
			rangecon=mod_seq
                  else:
		       rangecon=len(host)
	          for con in xrange(rangecon):
	              templist=[]
		      z=(loop*len(host))+con
                      templist.append(m_list[z])
	              p=multiprocessing.Process(target=worker,args=(host[con],port[con],templist)) 
	              jobs.append(p)
	              p.start()
                  for j in jobs:
		       j.join()
        else:
	      starts=0
	      rangeval=len(m_list)/len(host)
	      ends=rangeval
	      for con in xrange(len(host)):
	          templist=[]
	          for z in xrange(starts,ends):
		      templist.append(m_list[z])
                  starts=ends
		  ends=ends+rangeval
		  p=multiprocessing.Process(target=worker,args=(host[con],port[con],templist))
	          jobs.append(p)
		  p.start()

              for j in jobs:
                   j.join()

def createCCR(msisdn,tm_activation_id,session_id):
        sessionid=session_id.replace('\n', '')
        CCR_avps=[ ]
        CCR_avps.append(encodeAVP('Session-Id', sessionid))
        CCR_avps.append(encodeAVP('Destination-Realm', 'amdocs.com'))
        CCR_avps.append(encodeAVP('Auth-Application-Id', 4))
        CCR_avps.append(encodeAVP('CC-Request-Type', 4))
        CCR_avps.append(encodeAVP('CC-Request-Number', 1))
        CCR_avps.append(encodeAVP('Subscription-Id',[
                        encodeAVP('Subscription-Id-Data',msisdn), 
                        encodeAVP('Subscription-Id-Type', 0)
                                                ]))
        CCR_avps.append(encodeAVP('Multiple-Services-Credit-Control',[
			  encodeAVP('Rating-Group',91), 
                    	  encodeAVP('Requested-Service-Unit',[
	          	       encodeAVP('CC-Money',[
			       encodeAVP('Unit-Value',[
			       encodeAVP('Value-Digits',0)])])]),
			       encodeAVP('Ad-Hoc-Event-Attributes',[
			           encodeAVP('Cost-Code2',str(tm_activation_id))])
			   ]))
        CCR_avps.append(encodeAVP('Requested-Action',1))
        CCR_avps.append(encodeAVP('TMOffline-Ind',str('Y')))
        # Create message header (empty)
        CCR=HDRItem()
        # Set command code
        CCR.cmd=dictCOMMANDname2code('Credit-Control')
        # Set Hop-by-Hop and End-to-End
        initializeHops(CCR)
        # Add AVPs to header and calculate remaining fields
        msg=createReq(CCR,CCR_avps)
	time.sleep(0.1)
        return msg 
#end of routine send_ccr_auth_messages()

config = ConfigParser.RawConfigParser()
config_file=os.path.join(app_path,'olc9_talkmania_act.config')
config.read(config_file)
threshold=int(config.get('others_section','MAX_RECORD'))
sleeps=int(config.get('others_section','SLEEP'))
start_Peak_Hours=int(config.get('others_section','START_PEAK_HOURS'))
end_Peak_Hours=int(config.get('others_section','END_PEAK_HOURS'))
proc_count=int(config.get('others_section','CPU_COUNT'))
f=open(config_file,'r')
data=f.read()
host_list=re.findall(r'\bHOST_%s_FR[1-9]*'%site,data)
port_list=re.findall(r'\bPORT_%s_FR[1-9]*'%site,data)
host=[]
port=[]
thread=list()
threadDict={}
fr_section='FR_SECTION_'+site
for h in host_list:
    host.append(config.get(fr_section,h))

for p in port_list:
    port.append(config.get(fr_section,p))
if site=='JKT':
    list_of_files=glob.glob(file_path_jkt)
else:
    list_of_files=glob.glob(file_path)
for filename in  list_of_files:
    pstart=0
    pend=0
    ptemp=0
    currtime2=time.strftime("%H:%M", localtime)
    split_Time2=currtime2.split(':')
    hours2=int(split_Time2[0])
    minutes2=int(split_Time2[1])

    if (os.path.exists(stop_path)):
	 succ_Count=sum(1 for line in open(succ_Filename))
	 fail_Count=sum(1 for line in open(fail_Filename))
	 total_Count=succ_Count + fail_Count
	 copyfile("%s/Stat_Temp_%s.sql"%(log_stat,site),"%s/New_Stat_Temp_%s.sql"%(log_stat,site))
	 stat_temp=open("%sStat_Temp_%s.sql"%(log_stat,site),"a+")
         endtime=time.localtime()
	 end_Time=time.strftime("%Y%m%d%H%M%S", endtime)
         stat_temp.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
	 stat_temp.close()
	 config = ConfigParser.RawConfigParser()
	 config_file=os.path.join(app_path,'olc9_talkmania_act.config')
	 config.read(config_file)
	 sqlloader=config.get('others_section','SQLLOADER')
	 sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,"%sStat_Temp_%s.sql"%(log_stat,site))
         subprocess.call('%s'%sqlloader_str, shell=True)

         os.remove("%s/Stat_Temp_%s.sql"%(log_stat,site)) 
	 os.rename("%s/New_Stat_Temp_%s.sql"%(log_stat,site),"%s/Stat_Temp_%s.sql"%(log_stat,site))
         raise Exception('Process has been stopped!!!')
         sys.exit(1)
    count_Threshold=sum(1 for line in open(control_threshold))
    if(count_Threshold>=threshold):
	f=open(control_threshold,'r+')
	f.truncate()
	time.sleep((60-minutes2)*60)
    os.rename(filename,filename+".inp")
    linecount = sum(1 for line in open(filename+".inp"))
    message_queue=multiprocessing.Queue()
    list_messge=[]
    pdiv=linecount/proc_count
    modseq=linecount%proc_count
    pjobs=[]
    if modseq>0:
	prange=pdiv+1
        for x in range(proc_count):
            pstart=ptemp
	    if x==(proc_count-1):
	       pend=linecount
            else:
	       pend=(x*prange)+prange
	    ptemp=pend
	    q=multiprocessing.Process(target=worker2,args=(filename+".inp",pstart,pend))
	    pjobs.append(q)
	    q.start()
    else:
	prange=pdiv
	for x in range(proc_count):
	    pstart=ptemp
	    pend=(x*prange)+prange
	    ptemp=pend
	    q=multiprocessing.Process(target=worker2,args=(filename+".inp",pstart,pend))
	    pjobs.append(q)
	    q.start()
    for j  in  pjobs:
	 j.join()
           
    os.rename(filename+".inp",filename+".done")
stat_Jkt=os.path.join(log_stat,'Stat_Temp_JKT.sql')
stat_Mkr=os.path.join(log_stat,'Stat_Temp_MKR.sql')
stat_Srb=os.path.join(log_stat,'Stat_Temp_SRB.sql')
stat_Pkb=os.path.join(log_stat,'Stat_Temp_PKB.sql')
stat_Plg=os.path.join(log_stat,'Stat_Temp_PLG.sql')
stat_Bjm=os.path.join(log_stat,'Stat_Temp_BJM.sql')
endtime=time.localtime()
end_Time=time.strftime("%Y%m%d%H%M%S", endtime)
succ_Count=sum(1 for line in open(succ_Filename))
fail_Count=sum(1 for line in open(fail_Filename))
total_Count=succ_Count + fail_Count
sqlloader=config.get('others_section','SQLLOADER')
if site=='JKT':
     stat_Jkt=open(stat_Jkt,"a+")
     stat_Jkt.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
     stat_Jkt.close()
     sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,log_stat+'Stat_Temp_JKT.sql')
     subprocess.call('%s'%sqlloader_str, shell=True)
if site=='MKR':
     stat_Mkr=open(stat_Mkr,"a+")
     #stat_Mkr.write('to_date(\'%s\',\'yyyymmddhh24miss\'),%d,%d,%d);\ncommit;\n'%(end_Time,counter,succ_Count,fail_Count))
     stat_Mkr.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
     stat_Mkr.close()
     sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,log_stat+'Stat_Temp_MKR.sql')
     subprocess.call('%s'%sqlloader_str, shell=True)
if site=='SRB':
     stat_Srb=open(stat_Srb,"a+")
     stat_Srb.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
     stat_Srb.close()
     sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,log_stat+'Stat_Temp_SRB.sql')
     subprocess.call('%s'%sqlloader_str, shell=True)
if site=='PKB':
     stat_Pkb=open(stat_Pkb,"a+")
     stat_Pkb.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
     stat_Pkb.close()
     sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,log_stat+'Stat_Temp_PKB.sql')
     subprocess.call('%s'%sqlloader_str, shell=True)
if site=='PLG':
     stat_Plg=open(stat_Plg,"a+")
     stat_Plg.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
     stat_Plg.close()
     sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,log_stat+'Stat_Temp_PLG.sql')
     subprocess.call('%s'%sqlloader_str, shell=True)
if site=='BJM':
     stat_Bjm=open(stat_Bjm,"a+")
     stat_Bjm.write('%s|%d|%d|%d\n'%(end_Time,total_Count,succ_Count,fail_Count))
     stat_Bjm.close()
     sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_stat_file,log_stat+'Stat_Temp_BJM.sql')
     subprocess.call('%s'%sqlloader_str, shell=True)

os.chdir(log_stat)
dfiles=glob.glob('*%s.sql'%site)
for filenames in dfiles:
    os.remove(filenames)
f=open(control_threshold,'r+')
f.truncate()

