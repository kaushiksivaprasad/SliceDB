'''
Created on Aug 9, 2014

'''
from slicedbms.env import SliceEnv
from slicedbms.db import SliceDb
from slicedbms.field import SliceField
from slicedbms.query import SliceQuery, SliceOp, SliceCondition
import os


class SliceClient(object):
    '''
    classdocs
    '''
    def __init__(self, params=None):
        '''
        Constructor
        '''
        self.env= SliceEnv()
    
    def run(self):
        loop = 1
        choice = "" #this variable holds the user's choice in the menu:
        while (loop == 1 ):
            choice = self.__mainMenu()
            if choice == '1':
                self.__createTablePrompt()
            elif choice == '2':
                self.__updateRecordPrompt()
            elif choice == '3':
                self.__addRecordPrompt()
            elif choice == '4':
                self.__bulkLoadPrompt()
            elif choice == '5':
                self.__displayJoinPrompt()
            elif choice == '6':
                self.__runQueryPrompt()
            elif choice == '7':
                self.__executeReport1()
            elif choice == 'x':
                loop = 0

            #if loop != 0:
                #print()
        print ("Good bye!")

    def __mainMenu(self):
        print "IMPORTANT... Pls add the .apd file in the Data folder..for bulk load to work correctly.."
        print ('Slice DBMS Menu ')
        print('')
        print ('1. Create database')
        print ('2. Update Record')
        print ('3. Add record')
        print ('4. Bulk load')
        print ('5. Display Join')
        print ('6. Run Query')
        print ('7. Generate Report 1 and 2')
        print ('x. Exit')
        print('')
        choice= raw_input ("Choose your option: ") 
        while str.lower(choice) not in ['x','1', '2', '3', '4','5','6','7','8']:
            print ('Invalid choice')
            choice = raw_input("Choose your option: ")
        return choice 
    
    # 1. Create database
    def __createTablePrompt(self):
            dbName= self.__getTableName()
            if dbName in self.env.schemaMap:
                print("Error: The given database already exists")
                return None
            # Enter fields count
            loop =1
            count=0
            while loop==1 :
                fieldsCount =  raw_input ("Enter number of table fields:")
                try:
                    count = int(fieldsCount)
                    loop = 0
                except ValueError:
                    print("Error: number of fields should be an integer number!")
                
 
            sliceFields= self.__getTableFields(count)
            primaryField= self.__getPrimaryKey(sliceFields)
            
#             sliceEnv=SliceEnv()
            self.env.createDb(dbName, sliceFields, primaryField)
            
    # 2. Update Record      
    def __updateRecordPrompt(self):
        tableName = self.__getTableName()
        db = None
        try:
            db = self.env.open(tableName)
        except:
            return
        
        if db.schema.primaryKey is None:
            print("Error: can not update database that does not have a primary key.")
            return
            
        sliceRecord = db.createRecord()
        for field,type in db.schema.columns.iteritems():

            fieldInput =  raw_input("Enter "+field+"["+type+"]" + " :").strip()
            fieldData = fieldInput
            if type ==  SliceDb.INT:
                if self.isInt(fieldInput):
                    fieldData = int(fieldInput)
                    sliceRecord.setInt(field, fieldData)
                else:
                    print("Error: the value is not integer.") 
                    return
            elif type == SliceDb.DOUBLE:
                    if self.isFloat(fieldInput):
                        fieldData = float(fieldInput)
                        sliceRecord.setDouble(field,fieldData)
                    else:
                        print("Error: the value is not float.")
                        return
            else:
                sliceRecord.setString(field,fieldData)
        db.set(sliceRecord)
        self.env.close(tableName)
    
    #3. Add record   
    def __addRecordPrompt(self):
        tableName = self.__getTableName()
        db = None
        try:
            db = self.env.open(tableName)
        except:
            print "Table doesnt exist.."
            return
        
        sliceRecord = db.createRecord()
        for field,type in db.schema.columns.iteritems():
            fieldInput =  raw_input("Enter "+field+"["+type+"]" + " :").strip()
            fieldData = fieldInput
            if type ==  SliceDb.INT:
                if self.isInt(fieldInput):
                    fieldData = int(fieldInput)
                    sliceRecord.setInt(field, fieldData)
                else:
                    print("Error: the value is not integer.") 
                    return
            elif type == SliceDb.DOUBLE:
                    if self.isFloat(fieldInput):
                        fieldData = float(fieldInput)
                        sliceRecord.setDouble(field,fieldData)
                    else:
                        print("Error: the value is not float.")
                        return
            else:
                sliceRecord.setString(field,fieldData)
        db.set(sliceRecord)
        self.env.close(tableName)
    
    # 4. delete record
#     def __getdeleteRecord(self):
#         # Todo : dgdfgdfdfdfdf
#         tableName = self.__getTableName()
#         key = input("Enter key value for the desire record:")
#         db = None
#         try:
#             db = self.env.open(tableName)
#         except:
#             print "Table doesnt exist.."
#             return
#         if db is not None:
#             print ("Length of db records: ",len(db.records))
#             record = db.getRecordByKey(tableName, key)
#             if record is not None:
#                 db.deleteRecord(tableName, record)
#                 print("The record has been successfully deleted.")
#                 print ("Length of db records: ",len(db.records))
        
    # 5. Bulk load
    def __bulkLoadPrompt(self):
            tableName = self.__getTableName()
            db = None
            try:
                db = self.env.open(tableName)
            except:
                print "Table doesnt exist.."
                return
            print "IMPORTANT... Pls add the .apd file in the Data folder..for bulk load to work correctly.."
            while True:
                fileName = raw_input("Enter file name for bulk loading e.i test.apd :")
                if len(fileName) > 0 and self.IsValidFilename(fileName):
                    break
                else:
                    print("Error: empty file name or invalid file name.")
            
            db.bulkLoad(fileName)       
            print("The file loaded successfully :)")
            self.env.close(tableName)
    
    # 6. Display Join   
    def __displayJoinPrompt(self):
            print "Enter the first db Name.."
            tableName = self.__getTableName()
            print "Enter the second db Name.."
            tableName1 = self.__getTableName()
            db = None
            db1 = None
            try:
                db = self.env.open(tableName)
                db1 = self.env.open(tableName1)
            except:
                print "Table doesnt exist.."
                return
            columnMap = db1.schema.columns.copy()
            columnMap.update(db.schema.columns)
            for key,val in columnMap.iteritems():
                print "Possible column name for joining ...%s[%s]" %(key,val)
            col = None
            while True:
                col = raw_input("Enter the column name to join : ").strip()
                if col in columnMap:
                    break  
                else:
                    print("Invalid column name..")
            newDb = db.join(db1, col)
            z = 1                              
            for i in range(newDb.getRowCount()):
                print "Rec No : %d" % (z)
                sliceRec = newDb.get(i)
                for y in newDb.schema.columns:
                    if newDb.schema.columns[y] == SliceDb.INT:
                        print "Col %s: %d" % (y, sliceRec.getInt(y))
                    elif newDb.schema.columns[y] == SliceDb.STRING:
                        print "Col %s: %s" % (y, sliceRec.getString(y))
                    elif newDb.schema.columns[y] == SliceDb.DOUBLE:
                        print "Col %s: %f" % (y, sliceRec.getDouble(y))
                    z = z + 1
            self.env.close(tableName)
            self.env.close(tableName1)
    
    # 7. Run Query       
    def __runQueryPrompt(self):
        tableName = self.__getTableName()
        db = None
        try:
            db = self.env.open(tableName)
        except:
            print "Table doesnt exist.."
            return
        count = None
        while True:
            try:
                count = input("Enter the number of columns to display:")
                break
            except:
                print "Invalid input..pls input integers."
        for key,val in db.schema.columns.iteritems():
            print "Possible column names ...%s[%s]" %(key,val)
        print "Enter the columns that need to be displayed : "
        columns = set()
        for i in range(count):
            col = raw_input("Enter the column name : ").strip()
            if col in db.schema.columns:
                columns.add(col)  
            else:
                print("Invalid column name..")
                i = i - 1
        condition1 = SliceCondition()
        print "Enter the condition : "
        print "Possible Values : %s, %s and %s" %(SliceOp.EQ,SliceOp.GT,SliceOp.LT)
        condition = None
        while True:
            try:
                condition = raw_input("Enter the condition..").strip()
                if(condition == SliceOp.EQ or condition == SliceOp.GT or condition == SliceOp.LT):
                    condition1.addOp(condition)
                    break
            except:
                print "Invalid input..pls input a valid condition."
        col = None
        while True:
            try:
                col = raw_input("Pls enter the column on which the operation needs to be performed..").strip()
                if(col in db.schema.columns):
                    condition1.addColumn(col)
                    break
            except:
                print "Invalid input..pls input a valid column."
        colVal = None
        while True:
            if db.schema.columns[col] == SliceDb.INT:
                colVal = raw_input("Pls enter integer literal..").strip()
                try:
                    colVal = int(colVal)
                    break
                except:
                    print "Invalid input.."
            elif db.schema.columns[col] == SliceDb.DOUBLE:
                colVal = raw_input("Pls enter float literal..").strip()
                try:
                    colVal = float(colVal)
                    break
                except:
                    print "Invalid input.."
            else:
                colVal = raw_input("Pls enter string literal..").strip()
                if(len(colVal) > 0):
                    break
                print "Invalid input.."
        condition1.addLiteral(colVal)
        query = SliceQuery(columns, condition1)
        result = db.query(query)  
        z = 1                              
        for i in result:
            print "Rec No : %d" %(z)
            for y in columns:
                if db.schema.columns[y] == SliceDb.INT:
                    print "Col %s: %d" %(y,i.getInt(y))
                elif db.schema.columns[y] == SliceDb.STRING:
                    print "Col %s: %s" %(y,i.getString(y))
                elif db.schema.columns[y] == SliceDb.DOUBLE:
                    print "Col %s: %f" %(y,i.getDouble(y))
            z = z + 1
        self.env.close(tableName)
    # 8. Report 1               
    def __executeReport1(self):
            print('>>Report 1 and 2')
            print "IMPORTANT Both the reports in one exe.."
            print os.system("report.exe")
    # 9. Report 2        

    
    # 1. Create database: get Table Name   
    def __getTableName(self): 
        tableName= raw_input ("Enter table name: ").strip()
        
        while True:
            if len(tableName) == 0 :
                print ("Error: table name cannot be empty.")
            elif not str.isalpha(tableName):
                print ("Error: table name cannot contain white space,numeric, or special characters.")
            else:
                return tableName.strip() 
            tableName = raw_input("Enter table name: ").strip()
        
    
    # 1. Create database : get Fields Info
    def __getTableFields(self,count):
        fields=[]
        for i in range(0,count):
            fieldSchema = self.__getFieldNameAndType()
            sliceField=SliceField(fieldSchema[0], fieldSchema[1])
            fields.append(sliceField)     
        return fields   
    
    # 1. Create database : get Each Field name and type 
    def __getFieldNameAndType(self):
        fieldInfo= raw_input ("Enter field name followed by its type INT, STRING or FLOAT. i.e address|STRING : ").strip()
        while len(fieldInfo) == 0 :
            print ("Error: field name cannot be empty.")
            fieldInfo= raw_input ("Enter field name followed by its type INT, STRING or FLOAT. i.e address|STRING : ").strip()
        
        while True:
            values =  [x.strip() for x in fieldInfo.split('|')]
            invalidLength= len(values) != 2
            if not invalidLength :
                invalidColumnName = not str.isalpha( values[0]) and (len(values[0])==0)
                invalidType= str.upper(values[1]) not in ['INT', 'STRING','FLOAT']
                if  not (invalidLength or invalidColumnName or invalidType)  :
                    break
            print ("Error: field info can not be parsed. ")
            fieldInfo= raw_input ("Enter field name followed by its type INT, STRING or FLOAT. i.e address|STRING : ").strip()
        
        fieldName= values[0]
        fieldType = str.upper(values[1])
        if(fieldType == "INT"):
            fieldType = SliceDb.INT
        elif(fieldType == "STRING"):
            fieldType = SliceDb.STRING
        elif(fieldType == "FLOAT"):
            fieldType = SliceDb.DOUBLE
        return((fieldName,fieldType))
    
    # 1. Create database : get Primary Key
    def __getPrimaryKey(self,fields):
        columnNames =set()
        for field in fields:
            columnNames.add(field.name)
        
        while True:   
            primaryKey= raw_input ("Enter  primary field name or <Carriage return> for none: ").strip()
            if  len(primaryKey.strip()) == 0:
                return None 
            else:
                if  primaryKey.strip() not in columnNames:
                    print ("Error: the entry field is not member of table columns.")
                else:
                    return primaryKey.strip()
                     
    
    # 7. Run Query : get Display Columns
    def __getDisplayColumns(self):

        columnInfo= raw_input ("Enter display columns: enter column names to display. i.e name|age|address : ").strip()
        while len(columnInfo) == 0 :
            print ("Error: column name cannot be empty.")
            columnInfo= raw_input ("Enter display columns: enter column names to display. i.e name|age|address :").strip()

        while True:    
            values = [x.strip() for x in columnInfo.split('|')] 
            invalidLength= len(values) < 1
            if not invalidLength:
                invalidColumnName = not str.isalpha( values[0]) and (len(values[0])==0)
                if not (invalidLength or invalidColumnName) :
                    break
            print ("Error: condition entry can not be parsed.")
            columnInfo= raw_input ("Enter display columns: enter column names to display. i.e name|age|address : ").strip()
     
        columnNamesArray=values   
        return  columnNamesArray

                
    # 7. Run Query : get get Condition               
    def __getCondition(self):
        columnInfo= raw_input ("Enter condition: enter column name followed by its type EQ, GT or LT following by literal . i.e name|EQ|Joe Smith : ").strip()
        while len(columnInfo) == 0 :
            print ("Error: column name cannot be empty.")
            columnInfo= raw_input ("Enter condition: enter column name followed by its type EQ, GT or LT following by literal . i.e name|EQ|Joe Smith : ").strip()

        while True:    
            values =  [x.strip() for x in columnInfo.split('|')]
            invalidLength= len(values) != 3
            if not invalidLength:
                invalidColumnName = not str.isalpha( values[0].strip()) and (len(values[0].strip())==0)
                invalidType= str.upper(values[1].strip()) not in ['EQ', 'GT','LT']
                invalidLetral = len(values[2].strip())==0
                if not (invalidLength or invalidColumnName or invalidType or invalidLetral) :
                    break
            print ("Error: condition entry can not be parsed.")
            columnInfo= raw_input ("Enter condition: enter column name followed by its type EQ, GT or LT following by literal . i.e name|EQ|Joe Smith : ").strip()
                
                    
            
        columnName=values[0].strip()
        operation = str.upper(values[1].strip())
        literal = values[2].strip()
        
        condition = SliceCondition(columnName,operation,literal)              
        return  condition
    
    def isInt(self,strValue):
        try:
            if strValue == "" :
                return True
            int(strValue)
            return True
            print("Yes to Int")
        except ValueError:
            return False
            print("No no no  to int")
        
    def isFloat(self,strValue):
        try:
            if strValue == "" :
                return True
            float(strValue)
            return True
            print("Yes to Float")
        except ValueError:
            return False
            print("No Cant to Float") 
            
    def IsValidFilename(self,filename):
        return ((filename.find("\\") == -1) and (filename.find("/") == -1) and
                (filename.find(":") == -1) and (filename.find("*") == -1) and
                (filename.find("?") == -1) and (filename.find("\"") == -1) and
                (filename.find("<") == -1) and (filename.find(">") == -1) and
                (filename.find("|") == -1))

      
def main():  
    sliceClient = SliceClient()
    sliceClient.run()    


if __name__ == "__main__": 
    main()
