import flask
import socket
from flask import request, jsonify
from DataBaseOperations import *

app = flask.Flask(__name__)
app.config["DEBUG"] = True

configdata['NonMDMSearch']=''

@app.route('/', methods=['GET'])
def home():
	return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Welcome To "+socket.gethostname()+" "+request.method+"</h1><h2 style='color: red;text-align:center;' >Welcome To Web View Of Wallet Service Data</h2></body>"



#################################SALTLAKE#########################

@app.route('/GetWalletPromotions', methods=['GET'])
def GetWalletPromotions():
	formatStr=""
	data=GetDataFromMYSQL("SELECT * FROM "+configdata['DATABASE']['MYSQL']['NAME']+".PROMOTIONS;")
	for k in data:
		for idx in range(0,len(k)):
			#print(k[idx])
			formatStr+=str(k[idx])+" "
		formatStr+="<br><br>"
	#print(formatStr)
	return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Wallet Promotions..."+configdata['DATABASE']['MYSQL']['NAME']+"</h1><h2 style='color: red;text-align:center;' >"+formatStr+"</h2></body>"


@app.route('/GetCustomerDetails', methods=['GET'])
def GetCustomerDetails():
	formatStr=""
	data=GetDataFromMYSQL("SELECT * FROM "+configdata['DATABASE']['MYSQL']['NAME']+".CUSTOMER;")
	for k in data:
		for idx in range(0,len(k)):
			#print(k[idx])
			formatStr+=str(k[idx])+"|| "
		formatStr+="<br><br>"
	#print(formatStr)
	return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Customer Details..."+configdata['DATABASE']['MYSQL']['NAME']+"</h1><h2 style='color: red;text-align:center;' >"+formatStr+"</h2></body>"

#####Get Wallet Transactions for All

@app.route('/GetWalletTransactions', methods=['GET'])
def GetWalletTransactions():
	formatStr=""
	Database=configdata['DATABASE']['MYSQL']['NAME']
	#data=GetDataFromMYSQL("SELECT * FROM "+configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION;")
	data=GetDataFromMYSQL("select FirstName,LastName,MobileNumber,EnrollDate,RechargeDate,t.PromotionExpiryDate,RechargeAmount,t.PromotionAmount,PromotionCode,PromotionMinAmount,PromotionMaxAmount\
	From " +configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION t,"\
	+configdata['DATABASE']['MYSQL']['NAME']+".Customer c,"\
	+configdata['DATABASE']['MYSQL']['NAME']+".Promotions p\
	where t.customerid=c.customerid\
	and t.promotionid=p.promotionid\
	Order by MobileNumber")
	for k in data:
		for idx in range(0,len(k)):
			#print(k[idx])
			formatStr+=str(k[idx])+"|| "
		formatStr+="<br><br>"
	#print(formatStr)
	return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Wallet Transactions..."+configdata['DATABASE']['MYSQL']['NAME']+"</h1><h2 style='color: Tomato;text-align:center;' >"+formatStr+"</h2></body>"


@app.route('/GetWalletTransactionsForIndividual/<MobileNumber>', methods=['GET'])
def GetWalletTransactionsForIndividual(MobileNumber):
	formatStr=""
	#data=GetDataFromMYSQL("SELECT * FROM "+configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION WHERE CustomerID="+str(GetCustomerID(MobileNumber)))
	
	data=GetDataFromMYSQL("select FirstName,LastName,MobileNumber,EnrollDate,RechargeDate,t.PromotionExpiryDate,RechargeAmount,t.PromotionAmount,PromotionCode,PromotionMinAmount,PromotionMaxAmount\
	From " +configdata['DATABASE']['MYSQL']['NAME']+".WALLTETRANSACTION t,"\
	+configdata['DATABASE']['MYSQL']['NAME']+".Customer c,"\
	+configdata['DATABASE']['MYSQL']['NAME']+".Promotions p\
	where t.customerid=c.customerid\
	and t.promotionid=p.promotionid\
	and MobileNumber="	+MobileNumber+" Order by RechargeDate desc")
	
	for k in data:
		for idx in range(0,len(k)):
			#print(k[idx])
			formatStr+=str(k[idx])+"||"
		formatStr+="<br><br>"
	#print(formatStr)
	return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Wallet Transactions For Individual..."+configdata['DATABASE']['MYSQL']['NAME']+"</h1><h2 style='color: SlateBlue;text-align:center;' >"+formatStr+"</h2></body>"
#########  Create Entitiies in DataBase

@app.route('/CreateCustomer/', methods=['POST'])
def api_CreateCustomer():
	print(request)
	if len(request.args['MobileNumber'])<10:
		return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Error..."+"</h1><h2 style='color: red;text-align:center;' >Looks Like You Have Missed Few Digits of The Mobile Number</h2></body>"
		
	InsertStatemet="INSERT INTO "+configdata['DATABASE']['MYSQL']['NAME']+".CUSTOMER(FirstName,LastName,MobileNumber) values('"+request.args['FirstName']+"','"+request.args['LastName']+"','"+request.args['MobileNumber']+"')"
	print(InsertStatemet)
	CUD_MYSQLOperation(InsertStatemet)
	return jsonify(InsertStatemet)

@app.route('/RechargeWallet/', methods=['POST'])
def api_RechargeWallet():
	print(request)
	PromotionAmount=0
	if len(request.args['MobileNumber'])<10:
		return "<body bgcolor=powderblue><h1 style='color: blue;text-align:center;'>Error..."+"</h1><h2 style='color: red;text-align:center;' >Looks Like You Have Missed Few Digits of The Mobile Number</h2></body>"
	
	InsertValues=RechargeCustomerWallet(request.args['MobileNumber'],Decimal(request.args['RechargeAmount']))
	
	return (InsertValues)
	
###############################################################################################################    

app.run(socket.gethostname(),configdata['WEBAPI']['WebPort'],threaded=True)