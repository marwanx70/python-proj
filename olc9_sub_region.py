#!/usr/local/bin/python
import subprocess
import sys
import glob
import os
import os.path
sys.path.append(os.path.expanduser('~/pbin/lib'))
sys.path.append(os.path.expanduser('~/pbin/app'))
import cx_Oracle
import ConfigParser
import itertools
from libDiameter import *
from datetime import datetime
from itertools  import islice

#split file function
def split_File(startLine,endLine,tmpPath,sourceFile,destFile):
    tmpJoin=os.path.join(os.sep,tmpPath,sourceFile)
    file=open(tmpJoin,"r")
    file_Inprogress=open(destFile,'w+')
    for data in itertools.islice(file,startLine,endLine):
        file_Inprogress.write(data)
            

dat_File=''
fin_File=''
input_File=''
linenum_JKT=0
linenum_MKR=0
linenum_SRB=0
linenum_PKB=0
linenum_PLG=0
linenum_BJM=0
home_path=os.path.expandvars('$HOME')
home_path=os.path.expandvars('$HOME')
app_path=home_path+'/pbin/app'
file_path=home_path+'/var/tks/projs/olc/TalkMania/Input'
tmp_Path=home_path+'/var/tks/projs/olc/TalkMania/Temp'
work_Path=home_path+'/var/tks/projs/olc/TalkMania/Work'
log_path=home_path+'/var/tks/projs/olc/TalkMania/Log/'
control_file=os.path.join(app_path,'olc9_talkmania.ctl')
config = ConfigParser.RawConfigParser()
config_file=os.path.join(app_path,'olc9_talkmania_act.config')
config.read(config_file)
max_Record=int(config.get('others_section','SPLIT_RECORD'))
user_DB=config.get('others_section','USER_DB')
pass_DB=config.get('others_section','PASS_DB')
ip_DB=config.get('others_section','IP_DB')
input_Count=int(config.get('others_section','INPUT_COUNT'))
sqlloader=config.get('others_section','SQLLOADER')
ConnDB=ConnectDB(ip_DB,user_DB,pass_DB)
Query=ConnDB.cursor()
Query.execute('truncate table OLC9_TM_BULK')
os.chdir(file_path)
text_file_list=glob.glob("*.txt")
if len(text_file_list)==0:
    raise Exception('Input file isn\'t found')
    sys.exit(1)
print('=====Reading Input File=====\n')
for files in glob.glob("*.txt"):
	dat_File=files
        temp_Name=dat_File.split(".")
        fin_File=temp_Name[0]+".fin"
        input_File=temp_Name[0]
	rename_File=dat_File+".done"
	rename_fin_File=fin_File+".done"
	full_path=os.path.join(file_path,fin_File)
	rename_path=os.path.join(file_path,rename_File)
	rename_fin_path=os.path.join(file_path,rename_fin_File)
        if not os.path.isfile(full_path):
	    raise Exception('Fin File isn\'t found') 
	    sys.exit(1)
        num_lines = sum(1 for line in open(dat_File))
	if (num_lines > input_Count):
      	    raise Exception('Record count is %d and cross the threshold'%num_lines)
	    sys.exit(1)
        sqlloader_str='sqlldr userid=%s control=%s data=%s'%(sqlloader,control_file,file_path+'/'+dat_File)
        subprocess.call('%s'%sqlloader_str, shell=True)

	os.rename(file_path+"/"+dat_File,rename_path)   
	os.rename(file_path+"/"+fin_File,rename_fin_path)
stat_Jkt=os.path.join(log_path,'Stat_Temp_JKT.sql')
stat_Mkr=os.path.join(log_path,'Stat_Temp_MKR.sql')
stat_Srb=os.path.join(log_path,'Stat_Temp_SRB.sql')
stat_Pkb=os.path.join(log_path,'Stat_Temp_PKB.sql')
stat_Plg=os.path.join(log_path,'Stat_Temp_PLG.sql')
stat_Bjm=os.path.join(log_path,'Stat_Temp_BJM.sql')


filename_JKT=temp_Name[0]+'_JKT_'+datetime.now().strftime('%Y-%m-%d%H%M%S')+'.tmp'
filename_MKR=temp_Name[0]+'_MKR_'+datetime.now().strftime('%Y-%m-%d%H%M%S')+'.tmp'
filename_SRB=temp_Name[0]+'_SRB_'+datetime.now().strftime('%Y-%m-%d%H%M%S')+'.tmp'
filename_PKB=temp_Name[0]+'_PKB_'+datetime.now().strftime('%Y-%m-%d%H%M%S')+'.tmp'
filename_PLG=temp_Name[0]+'_PLG_'+datetime.now().strftime('%Y-%m-%d%H%M%S')+'.tmp'
filename_BJM=temp_Name[0]+'_BJM_'+datetime.now().strftime('%Y-%m-%d%H%M%S')+'.tmp'

path_Tmp_JKT=os.path.join(tmp_Path+'/JKT',filename_JKT)
path_Tmp_MKR=os.path.join(tmp_Path+'/MKR',filename_MKR)
path_Tmp_SRB=os.path.join(tmp_Path+'/SRB',filename_SRB)
path_Tmp_PKB=os.path.join(tmp_Path+'/PKB',filename_PKB)
path_Tmp_PLG=os.path.join(tmp_Path+'/PLG',filename_PLG)
path_Tmp_BJM=os.path.join(tmp_Path+'/BJM',filename_BJM)

file_Tmp_JKT=open(path_Tmp_JKT,'w+')
file_Tmp_MKR=open(path_Tmp_MKR,'w+')
file_Tmp_SRB=open(path_Tmp_SRB,'w+')
file_Tmp_PKB=open(path_Tmp_PKB,'w+')
file_Tmp_PLG=open(path_Tmp_PLG,'w+')
file_Tmp_BJM=open(path_Tmp_BJM,'w+')
print("=====Split File into Sites=====\n")
Query=ConnDB.cursor()
sql='select distinct msisdn,activation_id,description,l9_region from OLC9_TM_BULK,subscriber where msisdn=prim_resource_val and subscriber_type=:subtype'
Query.execute(sql,subtype='ALL')
for row in Query:
    record=row[0]+'|'+row[1]+'|'+row[2]+'\n'
    region=row[3]
    if region=='JKT':
        file_Tmp_JKT.write(record)
	linenum_JKT+=1
    elif region=='MKR':
        file_Tmp_MKR.write(record)
	linenum_MKR+=1
    elif region=='SRB':
	file_Tmp_SRB.write(record)
	linenum_SRB+=1
    elif region=='PKB':
	file_Tmp_PKB.write(record)
	linenum_PKB+=1
    elif region=='PLG':
	file_Tmp_PLG.write(record)
	linenum_PLG+=1
    elif region=='BJM':
	file_Tmp_BJM.write(record)
	linenum_BJM+=1
file_Tmp_JKT.close()
file_Tmp_MKR.close()
file_Tmp_SRB.close()
file_Tmp_PKB.close()
file_Tmp_PLG.close()
file_Tmp_BJM.close()
jkt_size=os.path.getsize(path_Tmp_JKT)
mkr_size=os.path.getsize(path_Tmp_MKR)
srb_size=os.path.getsize(path_Tmp_SRB)
pkb_size=os.path.getsize(path_Tmp_PKB)
plg_size=os.path.getsize(path_Tmp_PLG)
bjm_size=os.path.getsize(path_Tmp_BJM)
starttime=time.localtime()
start_Time=time.strftime("%Y%m%d%H%M%S", starttime)
if (jkt_size>0):
    stat_Jkt=open(stat_Jkt,"w+")
    #stat_Jkt.write("insert into OLC9_STAT_TM_BULK values(\'%s\',\'%s\','JAKARTA',to_date(\'%s\',\'yyyymmddhh24miss\'),"%(dat_File,filename_JKT,start_Time))
    stat_Jkt.write('%s|%s|JAKARTA|%s|'%(dat_File,filename_JKT,start_Time))
    stat_Jkt.close()
if (mkr_size>0):
    stat_Mkr=open(stat_Mkr,"w+")
    stat_Mkr.write('%s|%s|MAKASSAR|%s|'%(dat_File,filename_MKR,start_Time))
    stat_Mkr.close()
if (srb_size>0):
    stat_Srb=open(stat_Srb,"w+")
    stat_Srb.write('%s|%s|SURABAYA|%s|'%(dat_File,filename_SRB,start_Time))
    stat_Srb.close()
if (pkb_size>0):
    stat_Pkb=open(stat_Pkb,"w+")
    stat_Pkb.write('%s|%s|PEKANBARU|%s|'%(dat_File,filename_PKB,start_Time))
    stat_Pkb.close()
if (plg_size>0):
    stat_Plg=open(stat_Plg,"w+")
    stat_Plg.write('%s|%s|PALEMBANG|%s|'%(dat_File,filename_PLG,start_Time))
    stat_Plg.close()
if (bjm_size>0):
    stat_Bjm=open(stat_Bjm,"w+")
    stat_Bjm.write('%s|%s|BANJARMASIN|%s|'%(dat_File,filename_BJM,start_Time))
    stat_Bjm.close()

if(linenum_JKT > max_Record):
   prevSeq=0
   divLine=linenum_JKT/max_Record
   modSeq=linenum_JKT%max_Record
   if(modSeq>0):
        for seq in range(1,divLine+2):
           path_Work_JKT=os.path.join(work_Path+'/JKT','TMBulk_JKT_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_JKT,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/JKT',filename_JKT,path_Work_JKT)
	   prevSeq=seq
   else:
       for seq in range(1,divLine+1):
	   path_Work_JKT=os.path.join(work_Path+'/JKT','TMBulk_JKT_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_JKT,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/JKT',filename_JKT,path_Work_JKT)
	   prevSeq=seq
if(linenum_MKR > max_Record):
   prevSeq=0
   divLine=linenum_MKR/max_Record
   modSeq=linenum_MKR%max_Record
   if(modSeq>0):
        for seq in range(1,divLine+2):
           path_Work_MKR=os.path.join(work_Path+'/MKR','TMBulk_MKR_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_MKR,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/MKR',filename_MKR,path_Work_MKR)
	   prevSeq=seq
   else:
       for seq in range(1,divLine+1):
	   path_Work_MKR=os.path.join(work_Path+'/MKR','TMBulk_MKR_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_MKR,'w+')
           split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/MKR',filename_MKR,path_Work_MKR)
           prevSeq=seq
if(linenum_SRB > max_Record):
   prevSeq=0
   divLine=linenum_SRB/max_Record
   modSeq=linenum_SRB%max_Record
   if(modSeq>0):
        for seq in range(1,divLine+2):
           path_Work_SRB=os.path.join(work_Path+'/SRB','TMBulk_SRB_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_SRB,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/SRB',filename_SRB,path_Work_SRB)
	   prevSeq=seq
   else:
       for seq in range(1,divLine+1):
	   path_Work_SRB=os.path.join(work_Path+'/SRB','TMBulk_SRB_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_SRB,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/SRB',filename_SRB,path_Work_SRB)
	   prevSeq=seq
if(linenum_PKB > max_Record):
   prevSeq=0
   divLine=linenum_PKB/max_Record
   modSeq=linenum_PKB%max_Record
   if(modSeq>0):
        for seq in range(1,divLine+2):
           path_Work_PKB=os.path.join(work_Path+'/PKB','TMBulk_PKB_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_PKB,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/PKB',filename_PKB,path_Work_PKB)
	   prevSeq=seq
   else:
       for seq in range(1,divLine+1):
	   path_Work_PKB=os.path.join(work_Path+'/PKB','TMBulk_PKB_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_PKB,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/PKB',filename_PKB,path_Work_PKB)
	   prevSeq=seq
if(linenum_PLG > max_Record):
   prevSeq=0
   divLine=linenum_PLG/max_Record
   modSeq=linenum_PLG%max_Record
   if(modSeq>0):
        for seq in range(1,divLine+2):
           path_Work_PLG=os.path.join(work_Path+'/PLG','TMBulk_PLG_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_PLG,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/PLG',filename_PLG,path_Work_PLG)
	   prevSeq=seq
   else:
       for seq in range(1,divLine+1):
	   path_Work_PLG=os.path.join(work_Path+'/PLG','TMBulk_PLG_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_PLG,'w+')
           split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/PLG',filename_PLG,path_Work_PLG)
	   prevSeq=seq
if(linenum_BJM > max_Record):
   prevSeq=0
   divLine=linenum_BJM/max_Record
   modSeq=linenum_BJM%max_Record
   if(modSeq>0):
        for seq in range(1,divLine+2):
           path_Work_BJM=os.path.join(work_Path+'/BJM','TMBulk_BJM_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_BJM,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/BJM',filename_BJM,path_Work_BJM)
	   prevSeq=seq
   else:
       for seq in range(1,divLine+1):
	   path_Work_BJM=os.path.join(work_Path+'/BJM','TMBulk_BJM_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_'+str(seq)+'_.ready')
	   #open(path_Work_BJM,'w+')
	   split_File((prevSeq*max_Record),seq*max_Record,tmp_Path+'/BJM',filename_BJM,path_Work_BJM)
	   prevSeq=seq
print ('=====Split and Transfer  File =====\n')
if((linenum_JKT <= max_Record) and (linenum_JKT<>0)):
    path_Work_JKT=os.path.join(work_Path+'/JKT','TMBulk_JKT_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_1_.ready')
    open(path_Work_JKT,'w+')
    split_File(0,linenum_JKT,tmp_Path+'/JKT',filename_JKT,path_Work_JKT)
if((linenum_MKR <= max_Record) and (linenum_MKR<>0)):
    path_Work_MKR=os.path.join(work_Path+'/MKR','TMBulk_MKR_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_1_.ready')
    open(path_Work_MKR,'w+')
    split_File(0,linenum_MKR,tmp_Path+'/MKR',filename_MKR,path_Work_MKR)
if((linenum_SRB <= max_Record) and (linenum_SRB<>0)):
    path_Work_SRB=os.path.join(work_Path+'/SRB','TMBulk_SRB_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_1_.ready')
    open(path_Work_SRB,'w+')
    split_File(0,linenum_SRB,tmp_Path+'/SRB',filename_SRB,path_Work_SRB)
if((linenum_PKB <= max_Record) and (linenum_PKB<>0)):
    path_Work_PKB=os.path.join(work_Path+'/PKB','TMBulk_PKB_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_1_.ready')
    open(path_Work_PKB,'w+')
    split_File(0,linenum_PKB,tmp_Path+'/PKB',filename_PKB,path_Work_PKB)
if((linenum_PLG <= max_Record) and (linenum_PLG<>0)):
    path_Work_PLG=os.path.join(work_Path+'/PLG','TMBulk_PLG_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_1_.ready')
    open(path_Work_PLG,'w+')
    split_File(0,linenum_PLG,tmp_Path+'/PLG',filename_PLG,path_Work_PLG)
if((linenum_BJM <= max_Record) and (linenum_BJM<>0)):
    path_Work_BJM=os.path.join(work_Path+'/BJM','TMBulk_BJM_'+input_File+'_'+datetime.now().strftime('%Y%m%d%H%M%S')+'_1_.ready')
    open(path_Work_BJM,'w+')
    split_File(0,linenum_BJM,tmp_Path+'/BJM',filename_BJM,path_Work_BJM)

if (jkt_size>0):
   ip_batch_jkt=config.get('BATCH_SECTION_JKT','BATCH_JKT_IP')
   username_batch_jkt=config.get('BATCH_SECTION_JKT','BATCH_JKT_USERNAME')
   path_batch_jkt=config.get('BATCH_SECTION_JKT','BATCH_JKT_PATH')
   log_batch_jkt=config.get('BATCH_SECTION_JKT','BATCH_JKT_LOG')
   os.system("scp %s %s@%s:%s"%(work_Path+'/JKT/*ready',username_batch_jkt,ip_batch_jkt,path_batch_jkt))
   os.system("scp %s %s@%s:%s"%(log_path+'Stat_Temp_JKT.sql',username_batch_jkt,ip_batch_jkt,log_batch_jkt))
   os.chdir(work_Path+'/JKT')
   dfiles=glob.glob('*ready')
   for filenames in dfiles:
       os.remove(filenames)
   #os.remove(log_path+'Stat_Temp_JKT.sql')
if (mkr_size>0):
   ip_batch_mkr=config.get('BATCH_SECTION_MKR','BATCH_MKR_IP')
   username_batch_mkr=config.get('BATCH_SECTION_MKR','BATCH_MKR_USERNAME')
   path_batch_mkr=config.get('BATCH_SECTION_MKR','BATCH_MKR_PATH')
   log_batch_mkr=config.get('BATCH_SECTION_MKR','BATCH_MKR_LOG')
   os.system("scp %s %s@%s:%s"%(work_Path+'/MKR/*ready',username_batch_mkr,ip_batch_mkr,path_batch_mkr))
   os.system("scp %s %s@%s:%s"%(log_path+'Stat_Temp_MKR.sql',username_batch_mkr,ip_batch_mkr,log_batch_mkr))
   os.chdir(work_Path+'/MKR')
   dfiles=glob.glob('*ready')
   for filenames in dfiles:
       os.remove(filenames)
   os.remove(log_path+'Stat_Temp_MKR.sql') 
if (srb_size>0):
   ip_batch_srb=config.get('BATCH_SECTION_SRB','BATCH_SRB_IP')
   username_batch_srb=config.get('BATCH_SECTION_SRB','BATCH_SRB_USERNAME')
   path_batch_srb=config.get('BATCH_SECTION_SRB','BATCH_SRB_PATH')
   log_batch_srb=config.get('BATCH_SECTION_SRB','BATCH_SRB_LOG')
   os.system("scp %s %s@%s:%s"%(work_Path+'/SRB/*ready',username_batch_srb,ip_batch_srb,path_batch_srb))
   os.system("scp %s %s@%s:%s"%(log_path+'Stat_Temp_SRB.sql',username_batch_srb,ip_batch_srb,log_batch_srb))
   os.chdir(work_Path+'/SRB')
   dfiles=glob.glob('*ready')
   for filenames in dfiles:
       os.remove(filenames)
   os.remove(log_path+'Stat_Temp_SRB.sql')
if (pkb_size>0):
   ip_batch_pkb=config.get('BATCH_SECTION_PKB','BATCH_PKB_IP')
   username_batch_pkb=config.get('BATCH_SECTION_PKB','BATCH_PKB_USERNAME')
   path_batch_pkb=config.get('BATCH_SECTION_PKB','BATCH_PKB_PATH')
   log_batch_pkb=config.get('BATCH_SECTION_PKB','BATCH_PKB_LOG')
   os.system("scp %s %s@%s:%s"%(work_Path+'/PKB/*ready',username_batch_pkb,ip_batch_pkb,path_batch_pkb))
   os.system("scp %s %s@%s:%s"%(log_path+'Stat_Temp_PKB.sql',username_batch_pkb,ip_batch_pkb,log_batch_pkb))
   os.chdir(work_Path+'/PKB')
   dfiles=glob.glob('*ready')
   for filenames in dfiles:
       os.remove(filenames)
   os.remove(log_path+'Stat_Temp_PKB.sql')
if (plg_size>0):
   ip_batch_plg=config.get('BATCH_SECTION_PLG','BATCH_PLG_IP')
   username_batch_plg=config.get('BATCH_SECTION_PLG','BATCH_PLG_USERNAME')
   path_batch_plg=config.get('BATCH_SECTION_PLG','BATCH_PLG_PATH')
   log_batch_plg=config.get('BATCH_SECTION_PLG','BATCH_PLG_LOG')
   os.system("scp %s %s@%s:%s"%(work_Path+'/PLG/*ready',username_batch_plg,ip_batch_plg,path_batch_plg))
   os.system("scp %s %s@%s:%s"%(log_path+'Stat_Temp_PLG.sql',username_batch_plg,ip_batch_plg,log_batch_plg))
   os.chdir(work_Path+'/PLG')
   dfiles=glob.glob('*ready')
   for filenames in dfiles:
       os.remove(filenames)
   os.remove(log_path+'Stat_Temp_PLG.sql')
if (bjm_size>0):
  ip_batch_bjm=config.get('BATCH_SECTION_BJM','BATCH_BJM_IP')
  username_batch_bjm=config.get('BATCH_SECTION_BJM','BATCH_BJM_USERNAME')
  path_batch_bjm=config.get('BATCH_SECTION_BJM','BATCH_BJM_PATH')
  log_batch_bjm=config.get('BATCH_SECTION_BJM','BATCH_BJM_LOG')
  os.system("scp %s %s@%s:%s"%(work_Path+'/BJM/*ready',username_batch_bjm,ip_batch_bjm,path_batch_bjm))
  os.system("scp %s %s@%s:%s"%(log_path+'Stat_Temp_BJM.sql',username_batch_bjm,ip_batch_bjm,log_batch_bjm))
  os.chdir(work_Path+'/BJM')
  dfiles=glob.glob('*ready')
  for filenames in dfiles:
      os.remove(filenames)
  #os.remove(log_path+'Stat_Temp_BJM.sql')


os.rename(path_Tmp_JKT,os.path.join(tmp_Path+'/JKT',filename_JKT+'.done'))
os.rename(path_Tmp_MKR,os.path.join(tmp_Path+'/MKR',filename_MKR+'.done'))
os.rename(path_Tmp_SRB,os.path.join(tmp_Path+'/SRB',filename_SRB+'.done'))
os.rename(path_Tmp_PKB,os.path.join(tmp_Path+'/PKB',filename_PKB+'.done'))
os.rename(path_Tmp_PLG,os.path.join(tmp_Path+'/PLG',filename_PLG+'.done'))
os.rename(path_Tmp_BJM,os.path.join(tmp_Path+'/BJM',filename_BJM+'.done'))


#Query.execute('truncate table OLC9_TM_BULK')
ConnDB.close()
print ('=====Transfer File Done=====\n')
