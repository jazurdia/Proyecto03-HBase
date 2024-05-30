import Hbase as b
import os

os.system("cls")
res = b.scan('tablaGrande', startrow='row5', stoprow='row8')
for each in res:
    print(f"\n{each}")