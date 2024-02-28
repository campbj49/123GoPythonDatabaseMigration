import pandas as pd

#load the marked up sheet
markedUpSheet = pd.read_excel("ExcelSheets/Items_Markup.xlsx", header=None, index_col=0)

#create blank sheet to be filled out by the rest of the logic
migrationSheet = pd.DataFrame()


#loop through each of the sheet's columns
for column in markedUpSheet.columns:
    #process each column's properties, constructing the definition header if the column is active
    if markedUpSheet.at["Active",column]:
        print("This column is active: "+markedUpSheet.at["name",column])
        if not markedUpSheet.at["name",column] or not markedUpSheet.at["dataType",column]:
            raise Exception("Column " + column + " is missing its name and/or dataType")
        
        defString = f'name={markedUpSheet.at["name",column]},dataType={markedUpSheet.at["dataType",column]}'

        if markedUpSheet.at["dataType",column] == "entity":
            #if its an entity column construct the subentity file using the given values
            if not markedUpSheet.at["entityRef",column]:
                raise Exception("Entity type column " + column + " must have an entityRef")
            
        #get the column name and copy the rest of the data underneath the header and column name
        migrationSheet.at[0,column] = defString
        migrationSheet.at[1,column] = markedUpSheet.at["name",column]
        currentIndex = 2
        for val in markedUpSheet[column].tolist()[6:]:
            migrationSheet.at[currentIndex,column] = val
            currentIndex +=1

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