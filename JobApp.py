import click
from flask import Flask
from flask.cli import AppGroup
import configparser
import pyodbc
import asyncio
from datetime import datetime
import sys
import os
from pprint import pprint
import logging

app = Flask(__name__)

#Gets configuration
config = configparser.ConfigParser()
config.read(r'settings.config')
logging.basicConfig(filename=r'app.log', level=logging.DEBUG,format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')


#DataBase Class
class DataBase:
	def __init__(self):
		self.Connect()
		
	#Connects To DB
	def Connect(self):
		try:
			self.db = pyodbc.connect(Driver =  config['MSSQL']['Driver'],Server = config['MSSQL']['Server'],Database = config['MSSQL']['Database'])
		except Exception as Argument:
			print(config['ERRORS']['ConnectionError'])
			app.logger.error(str(Argument))
			
	#Selects Jobs
	def SelectAll(self):
		try :
			cursor = self.db.cursor()
			cursor.execute('SELECT Id, Name, Status, IsDeleted FROM dbo.Jobs WHERE IsDeleted=0')
			jobList = []
			jobList.append("Id, Name, Status, IsDeleted")
			for row in cursor:
				jobList.append(row)
			app.logger.info(config['LOGS']['SelectAllLog'])
			return jobList
		except Exception as Argument:
			print(config['ERRORS']['SelectionError'])
			app.logger.error(str(Argument))
			
	#Selects Job By Id
	def Select(self,Id):
		try:
			cursor = self.db.cursor()
			sql = 'SELECT Id, Name, Status, IsDeleted FROM dbo.Jobs WHERE IsDeleted=0 AND Id= %s' % Id
			result = cursor.execute(sql)
			return result.fetchone()
		except Exception as Argument:
			print(config['ERRORS']['SelectionError'])
			app.logger.error(str(Argument))
			return 0
			
	#Creates Table Jobs
	def Create(self):
		try:
			cursor = self.db.cursor()
			cursor.execute("DROP TABLE IF EXISTS Jobs") #Drops table if already exists
			sql = 'CREATE TABLE Jobs (Id INT IDENTITY(1,1) PRIMARY KEY, Name VarChar(20),CreatedDate DateTime, Status VarChar(10) , IsDeleted bit)'
			cursor.execute(sql)
			cursor.commit()
			app.logger.info(config['LOGS']['CreateLog'])
		except Exception as Argument:
			print(config['ERRORS']['CreateError'])
			app.logger.error(str(Argument))
			
	#Inserts a Job
	def Insert(self,name):
		try:
			cursor = self.db.cursor()
			sql = 'INSERT INTO Jobs(Name, Status, CreatedDate, IsDeleted) VALUES(?,?,?,?)'
			val = (name, "Idle", datetime.now(),0)
			cursor.execute(sql,val)
			cursor.commit()
			app.logger.info(config['LOGS']['InsertLog'])
		except Exception as Argument:
			print(config['ERRORS']['InsertError'])
			app.logger.error(str(Argument))
			
	#Updates a Job
	def Update(self,Id,Status):
		try:
			cursor = self.db.cursor()
			sql = 'UPDATE Jobs SET Status =? WHERE Id = ?'
			val = (Status,Id)
			cursor.execute(sql,val)
			cursor.commit()
			app.logger.info(config['LOGS']['UpdateLog'])
		except Exception as Argument:
			print(config['ERRORS']['UpdateError'])
			app.logger.error(str(Argument))
			
			
	#Deletes a Job
	def Delete(self,Id):
		try:
			cursor = self.db.cursor()
			sql = 'UPDATE Jobs SET IsDeleted = 1 WHERE Id = ?'
			val = (Id)
			cursor.execute(sql,val)
			cursor.commit()
			app.logger.info(config['LOGS']['DeleteLog'])
		except Exception as Argument:
			print(config['ERRORS']['DeleteError'])
			app.logger.error(str(Argument))
			
#Creates Jobs Table
@app.cli.command("CreateTable")
def CreateTable():
	DataBase().Create()
	print(config['MESSAGES']['TableMessage'])
	
#Adds A Job
@app.cli.command("AddJob")
@click.argument("name")
def AddJob(name):
	DataBase().Insert(name)
	pprint(DataBase().SelectAll())
#Lists Jobs
@app.cli.command("ListJobs")
def ListJobs():
	pprint(DataBase().SelectAll())

#Updates Job Status
@app.cli.command("UpdateJob")
def UpdateJob():
	pprint(DataBase().SelectAll())
	Id = input(config['MESSAGES']['IdRequest'])
	record = DataBase().Select(Id)
	if record != 0:
		if record != None:
			Status = input(config['MESSAGES']['StatusRequest'])
			DataBase().Update(Id,Status)
			pprint(DataBase().SelectAll())
		else:
			print(config['ERRORS']['NoRecordError'])
		
#Deletes A Job
@app.cli.command("DeleteJob")
def DeleteJob():
	pprint(DataBase().SelectAll())
	Id = input(config['MESSAGES']['IdRequest'])
	record = DataBase().Select(Id)
	if record != 0:
		if record != None:
			DataBase().Delete(Id)
			pprint(DataBase().SelectAll())
		else:
			print(config['ERRORS']['NoRecordError'])
	