import System.IO  
import Control.Monad
import Debug.Trace
import Data.List.Split
import Data.List

printMap :: Int -> Int -> [[Char]] -> [[[Char]]] -> [[[Char]]]
printMap i n list res=
	if i<n
		then do
			let x=list !! i
			let temp=[] ++ splitOn "|" x
			let t1= res ++ [temp]
			let newi=i+1
			printMap newi n list t1
	else do		
		res


vectorLength::[[Char]] -> Int
vectorLength a = do
	let temp=a !! 1
	read temp :: Int 
	

sortVectors::[[[Char]]]->[[[Char]]]
sortVectors list = sortBy compareVectors list

--rever::[(Int, Int)]->[(Int, Int)]
--rever [] = []
--rever (x:xs) = rever xs ++ [x]

compareVectors::[[Char]] ->[[Char]] ->Ordering
compareVectors a b 
    | vectorLength a <= vectorLength b = LT
    | vectorLength a > vectorLength b = GT

--main = do
--    print(map vectorLength [(1,4), (2,6), (-2, -8), (3, -4)])
--    print(sortVectors[(1,4), (2,6), (-2,-8), (3, -4)])
setElement:: [Char] -> Int -> Int -> [[[Char]]] -> [[[Char]]]
setElement element i j list= do
	let row= list !! i
	let x = take j row ++ [element] ++ drop (j+1) row
	let temp=take i list ++ [x] ++ drop (i+1) list
	temp

fun :: Int -> Int -> Int -> Float -> [[[Char]]] -> [[[Char]]] -> [[[Char]]]
fun i j n s res y= do
	let x1= y !! i
	let x2= y !! j
	if i==n || j==n then do
		let l=res ++ [x1]
		let idx=length l
		let idx1 =idx -1
		let a=show s
		let l1 =setElement a idx1 3  l
		l1
	else do
		let t1= x1 !! 1
		let t2= x2 !! 1
		let newj = j+1
		let val = read (x2!!3) :: Float
		if t1 == t2 then do
			let x = s + val
			let t1= x1 !! 1
			fun i newj n x res y
		else do
			let l=res ++ [x1]
			let idx=length l
			let idx1 =idx -1
			let a=show s
			let l1 =setElement a idx1 3  l
			let x=0.0
			fun j j n x l1 y	

printReport :: [[[Char]]] -> Int -> Int -> IO()
printReport report i n =do
	if i<n
		then do
			let entry=report !! i
			let custID= entry !! 1
			let sales= entry !! 3
			print(custID ++ "        " ++sales)
			let newi=i+1
			printReport report newi n
	else do
		print("Report Completed...")
		


processingFunction :: [[[Char]]] -> [[Char]] -> Int -> Int -> [[[Char]]] -> [[[Char]]]
processingFunction joinedDB listFromOrderDB i lengthOfJoinedDB outputList= do
	if i<lengthOfJoinedDB
		then do
			let entryFromJoinedDB = joinedDB !! i
			let lengthOfOrderDB = length listFromOrderDB
			let itemList = retrieveFunction entryFromJoinedDB listFromOrderDB 0 lengthOfOrderDB ""
			--Store the output for current customer and proceed for next customer
			let orderNum = entryFromJoinedDB !! 5
			let stringEntry = entryFromJoinedDB !! 1 ++ "    " ++ entryFromJoinedDB !! 5 ++ "    " ++ entryFromJoinedDB !! 6
			let age = entryFromJoinedDB !! 2
			let ageStr = "" ++ age
			let str = "" ++ orderNum
			let list1 = [] ++ [str]
			let list2 = [] ++ [stringEntry]
			let list3 = [] ++ [itemList]
			let list4 = [] ++ [ageStr]
			let innerList = list1 ++ list2 ++ list3 ++ list4
			let newOutputList = outputList ++ [innerList]
			let newi = i+1
			processingFunction joinedDB listFromOrderDB newi lengthOfJoinedDB newOutputList
	else do
		outputList
	

printReport2 :: [[[Char]]] -> Int -> Int -> IO()
printReport2 output i outputListLength= do
	if i<outputListLength
		then do
			let entry = output !! i
			let ageStr = entry !! 3
			let age= read ageStr :: Float
			let content = entry !! 1
			let items = entry !! 2
			if age>=43
				then do
					print(content ++ "    " ++ items)
			else do
				print ""
			let newi = i + 1
			printReport2 output newi outputListLength
	else do
		print "Report2 Completed..."
	

retrieveFunction :: [[Char]] -> [[Char]] -> Int -> Int -> [Char] -> [Char]
retrieveFunction entryFromJoinedDB listFromOrderDB i lengthOfOrderDB item= do
	let orderNumFromEntry = entryFromJoinedDB !! 5
	if	i<lengthOfOrderDB
		then do
			let entryFromOrderDB = listFromOrderDB !! i
			let dummyList = [] ++ splitOn "|" entryFromOrderDB
			let orderNumFromDummyList = dummyList !! 0
			if orderNumFromEntry==orderNumFromDummyList
				then do
					let newItem = dummyList!!1 
					let newItemList = item++","++newItem
					let newi = i+1
					retrieveFunction entryFromJoinedDB listFromOrderDB newi lengthOfOrderDB newItemList
			else do
				let newi = i+1
				let newItemList = item++""
				retrieveFunction entryFromJoinedDB listFromOrderDB newi lengthOfOrderDB newItemList
			
	else do
		item
			


addElement element i list= do
	let row= list !! i
	let x = row ++ [element]
	let temp=take i list ++ [x] ++ drop (i+1) list
	temp

join1 :: [[[Char]]] -> [[[Char]]] -> Int -> Int -> Int -> Int -> [[[Char]]] -> [[[Char]]]
join1 x y i j m n res=do
	if i==m || j==n then do
		res
	else do
		let x1= x !! i
		let x2= y !! j
		let t1= x1 !! 0
		let t2= x2 !! 1
		if t1==t2 then do
			let l= res ++ [x1]
			let idx=length l
			let idx1 =idx -1
			let val1=x2 !! 0
			let val2=x2 !! 2
			let val3=x2 !! 3
			let l1=addElement val1 idx1 l
			let l2=addElement val2 idx1 l1
			let l3=addElement val3 idx1 l2
			let newi=i+1
			let newj=j+1 
			join1 x y newi newj m n l3
		else do
			if t1 < t2 then do
				let newi = i + 1 
				join1 x y newi j m n res	
			else do
				let newj = j + 1 
				join1 x y i newj m n res	
			


main = do
	handle <- openFile "salesDB.slc" ReadMode
	contents <- hGetContents handle
	let singlewords = lines contents
	--print singlewords
	--print "================================================================="
	let n=length singlewords
	
	let res=[]
	let x=printMap 0 n singlewords res
	--print x
	let y=sortVectors x
	--print y
	let s=0.0
	let res=[]
	let r=fun 0 0 n s res y
	print r
	print "PRINTING REPORT..."
	let lengthOfRepAry= length r
	printReport r 0 lengthOfRepAry
	putStrLn "Report 1 is over"
	--Report 1 done !!!

	handle <- openFile "cusDB.slc" ReadMode
	contents <- hGetContents handle
	let singlewords = lines contents
	let m=length singlewords
	let res=[]
	let x=printMap 0 m singlewords res
	--print x

	let res=[]
	let joinedDB=join1 x y 0 0 m n res
	
	
	print "Printing REPORT2"
	print "............................................................"
	print "............................................................"
	handle <- openFile "OrderDB.slc" ReadMode
	contents <- hGetContents handle
	let listFromOrderDB = lines contents
	
	let lengthOfJoinedDB=length joinedDB
	let outputList = []
	print "Joined Table of Cust and Sales DB's..."
	print joinedDB
	print "............................................................"
	print "............................................................"
	print "OrderDB..."
	print listFromOrderDB
	print "............................................................"
	print "............................................................"
	
	let output = processingFunction joinedDB listFromOrderDB 0 lengthOfJoinedDB outputList
	---let orderNum = output !! 0
	---let content = output !! 1
	---let items = output !! 2
	---print(orderNum ++ "    " ++ content ++ "    " ++ items)
	print output
	let outputListLength = length output
	printReport2 output 0 outputListLength

