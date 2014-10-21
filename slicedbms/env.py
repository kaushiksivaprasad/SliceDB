from field import SliceField
from schemas import SliceSchema
import cPickle as pickle
import os
from db import SliceDb
sliceEnv = None
class SliceEnv:
    def __init__(self):
        self.schemaMap = dict()
        self.__doUnPickle()
        self.openDbs = dict()
    def createDb(self,dbName,schemaElements,primaryKey = None):
        if(isinstance(dbName, basestring) and (isinstance(primaryKey, basestring) or primaryKey == None)):
            if(type(schemaElements) == list and all(isinstance(a, SliceField) for a in schemaElements)):
                schema = SliceSchema(dbName)
                for item in schemaElements:
                    if(item.name is not None and item.type is not None):
                        val = item.name == primaryKey
                        schema.setColumnInfo(item.name, item.type, val)
                    else:
                        raise Exception("All the parameters are mandatory in SchemaField object")
                self.schemaMap[dbName] = schema
                self.__doPickle()
                self.__loadFile(dbName)
            else:
                raise Exception("The format of schema elements is wrong..")
        else:
            raise Exception("Invalid parameters..")
    def open(self,dbName):
        if(isinstance(dbName, basestring)):
            if(dbName in self.schemaMap):
                schema = self.schemaMap[dbName]
                db = SliceDb(schema)
                self.loadData(dbName+".slc", db)
                self.openDbs[dbName] = db
                return db
            else:
                raise Exception("No such db exists..")
        else:
            raise Exception("Invalid parameters..")
    def close(self,dbName):
        if(isinstance(dbName, basestring)):
            if(dbName in self.schemaMap and dbName in self.openDbs):
                self.__writeFile(self.__loadFile(dbName),dbName)
                del self.openDbs[dbName]
            else:
                raise Exception("No such db exists or the db is not open..")
        else:
            raise Exception("Invalid parameters..")
    def __doPickle(self):
        pickle.dump( self.schemaMap, open( "schemas.p", "w+" ) )
    def __doUnPickle(self):
        if(os.path.exists(r".\schemas.p")):
            self.schemaMap = pickle.load( open( "schemas.p", "r+" ) )
    def __loadFile(self,fileName):
        if(not os.path.exists(r".\Data")):                
            os.makedirs(r".\Data")
        return open(".\\Data\\"+fileName+".slc","w+")
    def loadData(self,fileName,db):
        file = open(".\\Data\\"+fileName,"r")
        cols = db.schema.columnOrder.split(",")
        dataIndex = dict()
        dataList = list()
        y = 0
        for content in file:
            dataInCols = content.strip().split("|")
            columnDataMap = dict()
            for i in range(len(cols)):
                if db.schema.columns[cols[i]] == SliceDb.INT:
                    int(dataInCols[i])
                elif db.schema.columns[cols[i]] == SliceDb.DOUBLE:
                    float(dataInCols[i])
                columnDataMap[cols[i]] = dataInCols[i]
                if db.schema.primaryKey == cols[i]:
                    dataIndex[dataInCols[i]] = y
            y = y + 1
            dataList.append(columnDataMap)
        db.dataIndex = dataIndex
        db.dataList = dataList
        return db
    def __writeFile(self,f,dbName):
        db = self.openDbs[dbName]
        strVal = ""
        columns = db.schema.columnOrder.split(",")
        for content in db.dataList:
            i = 0
            for cols in columns:
                if i > 0:
                    strVal = strVal+"|"
                strVal = strVal+str(content[cols]).strip()
                i = i + 1
            strVal = strVal + "\n"
        f.write(strVal)
                
        
        