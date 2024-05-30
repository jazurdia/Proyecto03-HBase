import Hbase as b

# crear dos tablas. 

b.create_table('example2', ['cf1', 'cf2'])
# b.create_table('example2', ['cf1', 'cf2'])

# insertar dos column qualifiers en las tablas



# example2 - cf1:col1
b.put('example2', 'row1', 'cf1', 'col1', 'table example 2 row1cf1col1')
b.put('example2', 'row2', 'cf1', 'col1', 'table example 2 row2cf1col1')
b.put('example2', 'row3', 'cf1', 'col1', 'table example 2 row3cf1col1')
b.put('example2', 'row4', 'cf1', 'col1', 'table example 2 row4cf1col1')
b.put('example2', 'row5', 'cf1', 'col1', 'table example 2 row5cf1col1')

# example2 - cf1:col2
b.put('example2', 'row1', 'cf1', 'col2', 'table example 2 row1cf1col2')
b.put('example2', 'row2', 'cf1', 'col2', 'table example 2 row2cf1col2')
b.put('example2', 'row3', 'cf1', 'col2', 'table example 2 row3cf1col2')
b.put('example2', 'row4', 'cf1', 'col2', 'table example 2 row4cf1col2')
b.put('example2', 'row5', 'cf1', 'col2', 'table example 2 row5cf1col2')

# example2 - cf2:col1
b.put('example2', 'row1', 'cf2', 'col1', 'table example 2 row1cf2col1')
b.put('example2', 'row2', 'cf2', 'col1', 'table example 2 row2cf2col1')
b.put('example2', 'row3', 'cf2', 'col1', 'table example 2 row3cf2col1')
b.put('example2', 'row4', 'cf2', 'col1', 'table example 2 row4cf2col1')
b.put('example2', 'row5', 'cf2', 'col1', 'table example 2 row5cf2col1')

# example2 - cf2:col2
b.put('example2', 'row1', 'cf2', 'col2', 'table example 2 row1cf2col2')
b.put('example2', 'row2', 'cf2', 'col2', 'table example 2 row2cf2col2')
b.put('example2', 'row3', 'cf2', 'col2', 'table example 2 row3cf2col2')
b.put('example2', 'row4', 'cf2', 'col2', 'table example 2 row4cf2col2')
b.put('example2', 'row5', 'cf2', 'col2', 'table example 2 row5cf2col2')



