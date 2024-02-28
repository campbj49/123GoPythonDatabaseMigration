import pandas as pd
from ExcelDBMigration import markupToImport

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

markupToImport(markedUpSheet)





#little comparrison script to see if my conversion is creating a result that matches the target

# targetSheet = pd.read_excel("ExcelSheets/Items_Migration_Trimmed.xlsx", header=None)

# totalCels = 0
# mismatchCels = 0
# for column in targetSheet.columns:
#     for index,val in enumerate(targetSheet[column].tolist()):
#         totalCels +=1
#         if column>=migrationSheet.shape[1] or index>=len(migrationSheet[column+1]):
#             mismatchCels +=1
#             print(f"Cell out of range in the migration sheet:{val}")
#         elif  val != migrationSheet.at[index,column+1] and not pd.isna(val):
#             print(f"Mismatched value at ({index}, {column})")
#             print(f"Target:{val}")
#             print(f"Result:{migrationSheet.at[index,column+1]}")
#             mismatchCels +=1
# print(f"There is a {(mismatchCels/totalCels)*100}% mismatch between the files")