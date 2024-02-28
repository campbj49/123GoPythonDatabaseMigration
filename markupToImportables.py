import pandas as pd

#function that adds the visibility groups to the end of a dataframe
def addVisGroups(sheet):
    #find the dimensions of the sheet:
    visGroupCol = sheet.shape[1]+1

    sheet.at[0,visGroupCol] = "name=visibilityGroups,dataType=array"
    sheet.at[1,visGroupCol] = "visibilityGroups"

    for index in range(2,sheet.shape[0]):
        sheet.at[index,visGroupCol] = -1

#load the marked up sheet
markedUpSheet = pd.read_excel("ExcelSheets/Items_Markup.xlsx", header=None, index_col=0)

#create blank sheet to be filled out by the rest of the logic
migrationSheet = pd.DataFrame()


#loop through each of the sheet's columns
for column in markedUpSheet.columns:
    #process each column's properties, constructing the definition header if the column is active
    if markedUpSheet.at["Active",column]:
        #declare the variables relavant to the whole column
        subSheet = False
        defStr = ""
        tableName = ""
        if not markedUpSheet.at["name",column] or not markedUpSheet.at["dataType",column]:
            raise Exception("Column " + column + " is missing its name and/or dataType")
        #iterate over whole active column, using the header rows to construct the defString
        for index, val in markedUpSheet[column].items():
            #string type indexes have the attributes needed for the defStr
            if type(index) == type("string") and val and not pd.isna(val) and (index != "Active" and index!="entityRef"):
                #attach the commas to the defStr at the top to prevent the trailing comma
                if defStr: defStr+=","

                #construct the subentity migration sheet
                if(index=="dataType" and val =="entity"):
                    val = "entity:" + markedUpSheet.at["entityRef",column]
                    subSheet = pd.DataFrame()
                    #construct the the file name and column name from entityRef input
                    [tableName, columnName] = markedUpSheet.at["entityRef",column].split(".")
                    subSheet.at[0,0] = f"name={columnName},dataType=text,updateCriteria=true"
                    subSheet.at[1,0] = columnName
                defStr+=f"{index}={val}"
            
            #-1 index marks the original column title. val can be igored and instead the top of the migration column can be set
            elif index == -1:
                migrationSheet.at[0,column] = defStr
                migrationSheet.at[1,column] = markedUpSheet.at["name",column]

            #nonnegative number type indexes have the actual values
            elif type(index) == type(1):
                migrationSheet.at[index+2,column] = val
                #if this is an entity type column unique values are put into the sheet being constructed
                if type(subSheet) == type(pd.DataFrame()) and val not in subSheet[0]:
                    print(val)
                    subSheet[0] = subSheet[0].add(val)
        #at the end of each column, if there was a subsheet constructed it can be saved to disk
        if type(subSheet) == type(pd.DataFrame()):
            addVisGroups(subSheet)
            subSheet.to_excel(f"{tableName}_Migration.xlsx")



        # print("This column is active: "+markedUpSheet.at["name",column])
        # if not markedUpSheet.at["name",column] or not markedUpSheet.at["dataType",column]:
        #     raise Exception("Column " + column + " is missing its name and/or dataType")
        
        # defString = f'name={markedUpSheet.at["name",column]},dataType={markedUpSheet.at["dataType",column]}'

        # if markedUpSheet.at["dataType",column] == "entity":
        #     #if its an entity column construct the subentity file using the given values
        #     if not markedUpSheet.at["entityRef",column]:
        #         raise Exception("Entity type column " + column + " must have an entityRef")
            
        # #get the column name and copy the rest of the data underneath the header and column name
        # migrationSheet.at[0,column] = defString
        # migrationSheet.at[1,column] = markedUpSheet.at["name",column]
        # currentIndex = 2
        # for val in markedUpSheet[column].tolist()[6:]:
        #     migrationSheet.at[currentIndex,column] = val
        #     currentIndex +=1


#add the visibility group column to the resulting sheet
addVisGroups(migrationSheet) 

#once all the rows have been process export the constructed dataframe
migrationSheet.to_excel("exported.xlsx")
print("Sheet successfully exported")





#little comparrison script to see if my conversion is creating a result that matches the target

targetSheet = pd.read_excel("ExcelSheets/Items_Migration_Trimmed.xlsx", header=None)

totalCels = 0
mismatchCels = 0
for column in targetSheet.columns:
    for index,val in enumerate(targetSheet[column].tolist()):
        totalCels +=1
        if column>=migrationSheet.shape[1] or index>=len(migrationSheet[column+1]):
            mismatchCels +=1
            print(f"Cell out of range in the migration sheet:{val}")
        elif  val != migrationSheet.at[index,column+1] and not pd.isna(val):
            print(f"Mismatched value at ({index}, {column})")
            print(f"Target:{val}")
            print(f"Result:{migrationSheet.at[index,column+1]}")
            mismatchCels +=1
print(f"There is a {(mismatchCels/totalCels)*100}% mismatch between the files")