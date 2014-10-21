
# import sys
# 
from slicedbms.record import SliceRecord
from slicedbms.db import SliceDb
from env import SliceEnv
from field import SliceField
from slicedbms.query import SliceCondition, SliceOp, SliceQuery
env = SliceEnv()
cols = [SliceField("cust",SliceDb.INT),SliceField("cust2",SliceDb.STRING),SliceField("cust3",SliceDb.DOUBLE)]
cols1 = [SliceField("cust",SliceDb.INT),SliceField("cust4",SliceDb.STRING),SliceField("cust5",SliceDb.DOUBLE)]
# env.createDb("su2", cols1)

# # env.createDb("su1", cols1)
db = env.open("su")
# db.bulkLoad("su.atd")

# env.close("su")
columns = set()
columns.add("cust")
columns.add("cust2")
condition = SliceCondition()
condition.addColumn("cust")
condition.addOp(SliceOp.EQ)
condition.addLiteral(4)
query = SliceQuery(columns, condition)
list = db.query(query)
for i in list:
    print i.getInt("cust")
    print i.getString("cust2")
    print i.getDouble("cust3")
env.close("su")



# sliceRec = db.createRecord()
# sliceRec.setString("cust4","kaushk")
# sliceRec.setInt("cust",4)
# sliceRec.setDouble("cust5",19.0)
# db.set(sliceRec)
# 
# sliceRec = db.createRecord()
# sliceRec.setString("cust4","kaushk")
# sliceRec.setInt("cust",5)
# sliceRec.setDouble("cust5",20.0)
# db.set(sliceRec)
# 
# sliceRec = db.createRecord()
# sliceRec.setString("cust4","kaushk")
# sliceRec.setInt("cust",1)
# sliceRec.setDouble("cust5",102.0)
# db.set(sliceRec)
# db1.join(db, "cust", "combined")
# env.close("su2")

# rec = db.get(2)
# print str(rec.getDouble("cust3"))
# print rec.getString("cust2")
# print str(rec.getInt("cust"))
# env.close("su")