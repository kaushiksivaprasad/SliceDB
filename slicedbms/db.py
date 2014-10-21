from slicedbms.record import SliceRecord
from slicedbms.schemas import SliceSchema
from slicedbms.query import SliceQuery, SliceOp
class SliceDb:
    INT = "INTEGER"
    STRING = "STRING"
    DOUBLE = "DOUBLE"
    def __init__(self,schema):
        self.schema = schema
        self.dataIndex = dict()
        self.dataList = list()
    def createRecord(self):
        return SliceRecord(self,None)
        pass
    def set(self,sliceRec):
        cols = self.schema.columnOrder.split(",")
        if sliceRec.primaryKeyVal is not None:
            if sliceRec.primaryKeyVal in self.dataIndex:
                colVal = self.dataList[self.dataIndex[sliceRec.primaryKeyVal]]
                self.__setData(colVal.keys(),sliceRec,colVal)
                return
            else:
                self.dataIndex[sliceRec.primaryKeyVal] = len(self.dataList)
        colVal = dict()
        self.__setData(cols,sliceRec,colVal)
        self.dataList.append(colVal)
        
    def __setData(self, iterator, sliceRec, dataStructure):
        for key in iterator:
            if(self.schema.columns[key] == SliceDb.STRING):
                dataStructure[key] = sliceRec.stringContents[key]
            elif self.schema.columns[key] == SliceDb.INT:
                dataStructure[key] = sliceRec.intContents[key]
            else:
                dataStructure[key] = sliceRec.doubleContents[key]
                
    def get(self, index,overRidePrimaryKey = None):
        if self.schema.primaryKey != None and not overRidePrimaryKey:
            index = str(index)
            if index in self.dataIndex:
                return SliceRecord(self, self.dataIndex[index])
            else:
                raise Exception("No such record exists..")
        else:
            if index < len(self.dataList):
                return SliceRecord(self, index)
            if index >= self.dataList:
                raise Exception("No record found at this position..")
        pass   
    def join(self,secondDb,columnName,newDbName = "temp"):
        if(columnName in self.schema.columns and columnName in secondDb.schema.columns):
            if(self.schema.columns[columnName] == secondDb.schema.columns[columnName]):
                newDb = SliceDb(self.__createNewSchema(secondDb,columnName, newDbName))
                secondDbIndex = dict()
                lastRecReadFromSecDb = 0
                for row in self.dataList:
                    val = row[columnName]
                    if(val in secondDbIndex):
                        aggregatedMap = secondDb.dataList[secondDbIndex[val]].copy()
                        aggregatedMap.update(row)
                        newDb.dataList.append(aggregatedMap)
                    else:
                        for i in range(lastRecReadFromSecDb,len(secondDb.dataList)):
                            rec = secondDb.dataList[i]
                            secDbColVal = rec[columnName]
                            secondDbIndex[secDbColVal] = i
                            lastRecReadFromSecDb = lastRecReadFromSecDb + 1
                            if(secDbColVal == val):
                                aggregatedMap = rec.copy()
                                aggregatedMap.update(row)
                                newDb.dataList.append(aggregatedMap)
                                break
                return newDb
            else:
                raise Exception("Both the columns doesn't have the same data type..")
        else:
            raise Exception("Both the tables specified doesn't contain the specified columnName")
    def __createNewSchema(self,secDb,colName,newDbName = "temp"):
        columnMap = secDb.schema.columns.copy()
        columnMap.update(self.schema.columns)
        secDbColOrder = secDb.schema.columnOrder
        indx = secDbColOrder.find(colName)
        if(indx > 0):
            secDbColOrder = secDbColOrder[0 : indx -1] + secDbColOrder[indx + len(colName) : ]
        else:
            secDbColOrder = secDbColOrder[indx + len(colName) + 1:]
        colOrder = self.schema.columnOrder +","+ secDbColOrder
        schema = SliceSchema(newDbName)
        schema.columns = columnMap
        schema.columnOrder = colOrder
        return schema
    
    def query(self,query):
        if(isinstance(query, SliceQuery)):
            if(all(a in self.schema.columns.keys() for a in query.columns) and query.condition.column in self.schema.columns.keys()):
                i = 0
                result = []
                for row in self.dataList:
                    val = None
                    if(self.schema.columns[query.condition.column] == SliceDb.INT):
                        val = int(row[query.condition.column])
                    elif(self.schema.columns[query.condition.column] == SliceDb.DOUBLE):
                        val = float(row[query.condition.column])
                    else:
                        val = row[query.condition.column]                    
                    if(query.condition.op == SliceOp.EQ):
                        if(val == query.condition.literal):
                            result.append(SliceRecord(self, i, query.columns)) 
                            break
                    elif(query.condition.op == SliceOp.GT):
                        if(val > query.condition.literal):
                            result.append(SliceRecord(self, i, query.columns))
                    else:
                        if(val < query.condition.literal):
                            result.append(SliceRecord(self, i, query.columns))
                    i = i + 1
                return result
            else:
                raise Exception("Columns dont match..")
        else:
            raise Exception("Invalid parameters..")
    def bulkLoad(self,fileName):
        from slicedbms.env import SliceEnv
        global SliceEnv
        SliceEnv().loadData(fileName, self)
    def getRowCount(self):
        return len(self.dataList)