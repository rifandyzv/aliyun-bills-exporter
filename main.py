import os
from billFunctions import *


client = APIClient.connect(os.getenv("ID"), os.getenv("KEY"))


startMonth="2023-10"

startBill = getBill(client, startMonth, "300", "1")
bills = exportAllBillstoCSV(client, startBill ,startMonth)

exportBillperProductCSV(bills)
exportBillperProductJSON(bills)