import pandas as pd
from ExcelDBMigration import markupToImport
import os

if "tmp_sheets" not in os.listdir():
    os.mkdir("tmp_sheets")

for file in os.listdir():
    if ".xlsx" in file:
        markedUpSheet = pd.read_excel("ExcelSheets/Items_Markup.xlsx", header=None, index_col=0)
        markupToImport(markedUpSheet, "MIGRATION_"+file)