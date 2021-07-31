#import pyodbc
#import time
import json
import datetime
import requests
import os
import zipfile
import shutil
import re
import sys
import math
import platform
from decimal import *

from pytz import timezone
import logging

starttime=datetime.datetime.now()

with open('Configuration.json') as json_file:
	configdata=json.load(json_file)

if os.name=='nt':
	configdata['OSTypePath']="\\"
	#configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']=configdata['LOCAL_FILE_PATH_WINDOW']
elif os.name=='posix':
	configdata['OSTypePath']="/"
	### Get Kerberos Ticket To Secure Login
	os.system("echo "+configdata['Password']+"|kinit "+configdata['Username'].split('\\')[1])

if configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH'][0:2]=="//": 
	configdata['OSTypePath']="/"

EntityName=configdata['SYSTEM_PARAMETERS']['ENTITY_NAME']

def EpochTimeConversion(Time,Format):
	utc_time = datetime.datetime.strptime(Time, Format)
	epoch_time = (utc_time - datetime.datetime(1970, 1, 1)).total_seconds()
	#print(epoch_time)
	return int(epoch_time)
	

def DBConnection():
	return MYSQLConnection()
	
def MYSQLConnection():
	import mysql.connector
	try:	
		cnxn= mysql.connector.connect(
		host=configdata['DATABASE']['MYSQL']['SERVER'],
		database=configdata['DATABASE']['MYSQL']['NAME'],
		user=configdata['DATABASE']['MYSQL']['USERNAME'],
		password=configdata['DATABASE']['MYSQL']['PASSWORD'])
		
		LogMessageInProcessLog("MYSQL DB Connection Succesfull")			
		
		return cnxn
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))


### Print Log Message	
def LogMessageInProcessLog(LogMessage):	
	
	if configdata['PROCESS_LOG']['ENABLE_FILE_LOGGING'].upper()=='YES':
		logging.error(LogMessage)
	if configdata['PROCESS_LOG']['PRINT_LOG_MESSGAE_ON_CONSOLE'].upper()=='YES':
		print(LogMessage)
	#	print("calll insid..",LogMessage)

### Log Configuration 

def SetProcessLogConfiguration():
	LogFilePath=configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']+configdata['OSTypePath']+"Logs"+configdata['OSTypePath']
		
	LogLevel=configdata['PROCESS_LOG']['LOG_LEVEL'].upper().replace("ERROR","40").replace("WARNING","30").replace("INFO","20").replace("DEBUG","10") ##ERROR 40|WARNING	30|INFO	20|DEBUG 10
	
	if not os.path.exists(LogFilePath):
		os.makedirs(LogFilePath)	
	
	LogFileName=LogFilePath+configdata['PROCESS_LOG']['LOG_FILE_NAME']+starttime.strftime('%Y%m%d')+".txt"
	
	if 	os.path.exists(LogFileName):
		CurrentLogFileSize=os.stat(LogFileName).st_size
		FileCreatedDaysBack= (EpochTimeConversion(str(datetime.datetime.now().astimezone(timezone(configdata['SYSTEM_PARAMETERS']['TIMEZONE']))).split('.')[0],'%Y-%m-%d %H:%M:%S')- os.stat(LogFileName).st_ctime) //86400
	
		if (FileCreatedDaysBack  >=configdata['PROCESS_LOG']['LOG_FILE_RETENTION_PERIOD'] or CurrentLogFileSize >= configdata['PROCESS_LOG']['LOG_FILE_MAX_FILE_SIZE']):
			print("\nLog File Is Deleted Current File Size <{}> Has Crossed The Permiiited Limit <{}>Big Or Its <{}> Days Old In System -Created Date <{}>\n".format(CurrentLogFileSize,configdata['LOG_FILE_MAX_FILE_SIZE'],FileCreatedDaysBack,time.ctime(os.stat(LogFileName).st_ctime)))
			try:
				os.remove(LogFileName)
			except Exception as e:
				print(e)
		
	logging.basicConfig(filename=LogFileName,format='%(asctime)s %(message)s', filemode='a',level=int(LogLevel))

def CUD_MYSQLOperation(QueryString):	
	try:
		coxn=DbConn		
		cursorr = coxn.cursor()
		cursorr.execute(QueryString)	
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		coxn.commit()
		#coxn.close()
		
def GetDataFromMYSQL(QueryString):		
	try:
		coxn=DbConn
		data=[]
		#coxn=DBConnection()		
		cursorr = coxn.cursor()
		cursorr.execute(QueryString)
		for j in cursorr:
			data.append(j)
			#print(data)
				
		#return str(data)#[1:-1] ## send table data as values()tuple
		return data # as list
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		#coxn.close()

#CUD_MYSQLOperation("delete from  WALLETSERVICE.CUSTOMER where FirstName='kk1'")
#CUD_MYSQLOperation("insert into  WALLETSERVICE.CUSTOMER(FirstName,LastName) values('kk1','pant1');")
#k=GetDataFromMYSQL("insert into  WALLETSERVICE.CUSTOMER(FirstName,LastName) values('kk1','pant1');")
#=GetDataFromMYSQL("select * from WALLETSERVICE.PROMOTIONS;")
'''
print(k)
for j in k:
	print(j[4],j[8])
exit(0)
'''

def InitApplication():

	CustomerTable="CREATE TABLE IF NOT EXISTS "+configdata['DATABASE']['MYSQL']['NAME']+".CUSTOMER"\
		"(\
		CustomerID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
		FirstName VARCHAR(50) NOT NULL ,\
		LastName  VARCHAR(50) NOT NULL ,\
		MobileNumber VARCHAR(13) NOT NULL ,\
		EnrollDate DATETIME DEFAULT CURRENT_TIMESTAMP(),\
		ExpiryDate DATETIME,\
		DeletionStatus char(1) DEFAULT 'N'\
		);"

	PromotionTable="CREATE TABLE IF NOT EXISTS "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS"\
		"(\
		PromotionID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
		PromotionCode VARCHAR(20),\
		PromotionAction VARCHAR(20),\
		PromotionDescription VARCHAR(50),\
		PromotionStartDate DATETIME DEFAULT CURRENT_TIMESTAMP(),\
		PromotionExpiryDate  DATETIME,\
		PromotionMinAmount DECIMAL(10,2),\
		PromotionMaxAmount DECIMAL(10,2),\
		PromotionAmount DECIMAL(10,2),\
		PromotionDuration INT,\
		AllowedRechargEvent INT,\
		PromotionPriority INT NOT NULL\
	);"


	WalletTransctionTable="CREATE TABLE IF NOT EXISTS "+configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION"\
		"(\
		WalletTranID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
		CustomerID INT REFERENCES WALLETSERVICE.CUSTOMER(CustomerID),\
		RechargeDate DATETIME DEFAULT CURRENT_TIMESTAMP(),\
		RechargeExpiryDate DATETIME,\
		RechargeAmount DECIMAL(10,2),\
		PromotionAmount DECIMAL(10,2),\
		PromotionID INT REFERENCES WALLETSERVICE.PROMOTIONS(PromotionID),\
		PromotionExpiryDate DATETIME\
		);"
	
	CUD_MYSQLOperation(CustomerTable)
	CUD_MYSQLOperation(PromotionTable)
	CUD_MYSQLOperation(WalletTransctionTable)


	Count=GetDataFromMYSQL("SELECT COUNT(1) FROM "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS")[0][0]
	#print(Count)
###Initial Data For Prmotion
	if Count==0:
		CUD_MYSQLOperation("INSERT INTO "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS(PromotionCode,PromotionAction,PromotionMinAmount,AllowedRechargEvent,PromotionAmount,PromotionDuration,PromotionPriority)  values('NA','NA',0,0,0,0,0);")			
		CUD_MYSQLOperation("INSERT INTO "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS(PromotionCode,PromotionAction,PromotionMinAmount,AllowedRechargEvent,PromotionAmount,PromotionDuration,PromotionPriority)  values('WELCOME','ADD',50,1,20,30,1);")
		CUD_MYSQLOperation("INSERT INTO "+configdata['DATABASE']['MYSQL']['NAME']+ ".PROMOTIONS(PromotionCode,PromotionAction,PromotionAmount,PromotionMaxAmount,PromotionDuration,PromotionPriority)  values('SALTSIDE','MULTIPLY',0.1,50,30,2);")

def GetCustomerID(MobileNumber):
	GetCustomerIDQuery="SELECT CustomerID FROM "+configdata['DATABASE']['MYSQL']['NAME']+".CUSTOMER WHERE MobileNumber="+MobileNumber
	return GetDataFromMYSQL(GetCustomerIDQuery)[0][0]

def GetRechargeCount(MobileNumber):
	GetCountQuery="SELECT count(CustomerID) FROM "+configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION WHERE CustomerID="+str(GetCustomerID(MobileNumber))
	return GetDataFromMYSQL(GetCountQuery)[0][0]

def GetPromotionDetail(CodeType,PromoItem):
	
	GetPromotionQuery="SELECT PromotionID,PromotionCode,PromotionAction,PromotionAmount,PromotionMinAmount,PromotionMaxAmount,PromotionDuration,PromotionPriority FROM "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS WHERE PromotionCode='"+CodeType+"'"
	
	#GetPromotionQuery="SELECT PromotionAmount FROM "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS"
	
	PromoData=GetDataFromMYSQL(GetPromotionQuery)
	for tupleitem in PromoData:
		if PromoItem.upper()=='PROMOTIONID':
			return str(tupleitem[0])
		if PromoItem.upper()=='PROMOTIONCODE':
			return str(tupleitem[1])
		if PromoItem.upper()=='PROMOTIONACTION':
			return str(tupleitem[2])
		if PromoItem.upper()=='PROMOTIONAMOUNT':
			return Decimal(str(tupleitem[3]))	
		if PromoItem.upper()=='PROMOTIONMINAMOUNT':
			return Decimal(str(tupleitem[4]))
		if PromoItem.upper()=='PROMOTIONMAXAMOUNT':
			return Decimal(str(tupleitem[5]))
		if PromoItem.upper()=='PROMOTIONDURATION':
			return int(str(tupleitem[6]))
		if PromoItem.upper()=='PROMOTIONPRIORITY':
			return int(str(tupleitem[7]))
	return 0 ### for no match
	
def GetApplicablePromotion(MobileNumber,RechargeAmount):
	NumberOfRecharge=GetRechargeCount(MobileNumber)
	PromoAmount=0
	print(NumberOfRecharge)	
	
	if NumberOfRecharge==0:
		if RechargeAmount>=GetPromotionDetail("WELCOME",'PROMOTIONMINAMOUNT'):
			if GetPromotionDetail("WELCOME",'PROMOTIONACTION').upper()=='ADD':
				PromoAmount=GetPromotionDetail("WELCOME",'PROMOTIONAMOUNT')
				return {"AppliedPromo":"WELCOME",
						"Amount":PromoAmount}
	#print(PromoAmount)
	
	PromoId=GetPromotionDetail("SALTSIDE",'PROMOTIONID')
	
	AccumlatedPromoSoFarQuery="SELECT IFNULL(sum(PromotionAmount),0) FROM "+configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION WHERE CustomerID="+str(GetCustomerID(MobileNumber))+" AND PromotionID="+str(PromoId)
	
	print(AccumlatedPromoSoFarQuery)
	TotalRechargeSoFar=GetDataFromMYSQL(AccumlatedPromoSoFarQuery)[0][0]
	
	#if NumberOfRecharge>0 and TotalRechargeSoFar<GetPromotionDetail("SALTSIDE",'PROMOTIONMAXAMOUNT'):	
	if TotalRechargeSoFar<GetPromotionDetail("SALTSIDE",'PROMOTIONMAXAMOUNT'):	

		if GetPromotionDetail("SALTSIDE",'PROMOTIONACTION').upper()=='MULTIPLY':
			PromoAmount=RechargeAmount*GetPromotionDetail("SALTSIDE",'PROMOTIONAMOUNT')
			if TotalRechargeSoFar+PromoAmount>GetPromotionDetail("SALTSIDE",'PROMOTIONMAXAMOUNT'):
				PromoAmount=GetPromotionDetail("SALTSIDE",'PROMOTIONMAXAMOUNT')-TotalRechargeSoFar
				
			return {"AppliedPromo":"SALTSIDE",
					"Amount":PromoAmount}
		
	return {"AppliedPromo":"NA","Amount":PromoAmount} ### if no match
	
	
def RechargeCustomerWallet(MobileNumber,RechargeAmount):
	PromotionData=GetApplicablePromotion(MobileNumber,RechargeAmount)
	
	CustomerID=GetCustomerID(MobileNumber)
	RechargeDate=datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")
	RechargeExpiryDate='Null'
	RechargeAmountFromCustomer=RechargeAmount
	PromotionAmount=PromotionData['Amount']
	PromotionID=GetPromotionDetail(PromotionData['AppliedPromo'],'PROMOTIONID')
	PromotionExpiryDate=(datetime.datetime.now()+datetime.timedelta(GetPromotionDetail(PromotionData['AppliedPromo'],'PROMOTIONDURATION'))).strftime("%y-%m-%d %H:%M:%S")
	PromotionPriority=GetPromotionDetail(PromotionData['AppliedPromo'],'PROMOTIONPRIORITY')
	
	#print(RechargeDate)
	InsertValues=str(CustomerID)+",'"+RechargeDate+"',"+str(RechargeExpiryDate)+","+str(RechargeAmount)+","+str(PromotionAmount)+","+str(PromotionID)+",'"+PromotionExpiryDate+"'"#"',"+str(PromotionPriority)
	
	print(InsertValues)
	
	CreateTransactionQuery="INSERT INTO "+configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION"\
	"(CustomerID,\
	RechargeDate,\
	RechargeExpiryDate,\
	RechargeAmount ,\
	PromotionAmount,\
	PromotionID ,\
	PromotionExpiryDate\
	) \
	values("+InsertValues+");"
	
	print(CreateTransactionQuery)
	CUD_MYSQLOperation(CreateTransactionQuery)
	return CreateTransactionQuery
####################MAIN#####################3

SetProcessLogConfiguration()
global DbConn
DbConn = DBConnection()	
InitApplication()
#DbConn.close()

#print(GetCustomerID('982345678'))
#print(GetRechargeCount('982345678'))
#print(GetPromotionDetail("NA",'PROMOTIONID'))
#print(RechargeCustomerWallet('982345678',50))