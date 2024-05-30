import Hbase as b
import os

os.system("cls")
res = b.scan('tablaGrande', startrow='row5', stoprow='row8')
for each in res:
    print(f"\n{each}")

print("\n\n")

print("b.scan('example2', columns=['cf1:col1', 'cf2:col1'])")
res = b.scan('example2', columns=['cf1:col1', 'cf2:col1'])
counter = 10
for row in res:
    if counter == 0:
        break
    print(row)
    counter -= 1



# hbase(main):005:0> scan 'my_table', {LIMIT => 10}
print("\n\n")
print("b.scan('tablaGrande', limit=10)")
res = b.scan('tablaGrande', limit=10)

for row in res:
    print(row)