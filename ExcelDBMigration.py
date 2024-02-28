import pandas as pd

def markupToImport(markedUpSheet):#create blank sheet to be filled out by the rest of the logic
    migrationSheet = pd.DataFrame()

    #loop through each of the sheet's columns
    for column in markedUpSheet.columns:
        #process each column's properties, constructing the definition header if the column is active
        if markedUpSheet.at["active",column]:
            #declare the variables relavant to the whole column
            subSheet = False
            defStr = ""
            tableName = ""
            curSubIndex = 0
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
                        curSubIndex = 2
                    defStr+=f"{index}={val}"
                
                #-1 index marks the original column title. val can be igored and instead the top of the migration column can be set
                elif index == -1:
                    migrationSheet.at[0,column] = defStr
                    migrationSheet.at[1,column] = markedUpSheet.at["name",column]

                #nonnegative number type indexes have the actual values
                elif type(index) == type(1):
                    migrationSheet.at[index+2,column] = val
                    #if this is an entity type column unique values are put into the sheet being constructed
                    if type(subSheet) == type(pd.DataFrame()) and val not in subSheet[0].unique():
                        subSheet.at[curSubIndex,0] =  val
                        curSubIndex += 1
            #at the end of each column, if there was a subsheet constructed it can be saved to disk
            if type(subSheet) == type(pd.DataFrame()):
                addVisGroups(subSheet)
                subSheet.to_excel(f"tmp_sheets/{tableName}_Migration.xlsx")


    #add the visibility group column to the resulting sheet
    addVisGroups(migrationSheet) 

    #once all the rows have been process export the constructed dataframe
    migrationSheet.to_excel("tmp_sheets/Migration.xlsx")
    print("Sheet successfully exported")

#function that adds the visibility groups to the end of a dataframe
def addVisGroups(sheet):
    #find the dimensions of the sheet:
    visGroupCol = sheet.shape[1]+1

    sheet.at[0,visGroupCol] = "name=visibilityGroups,dataType=array"
    sheet.at[1,visGroupCol] = "visibilityGroups"

    for index in range(2,sheet.shape[0]):
        sheet.at[index,visGroupCol] = -1