'''
Created on Aug 24, 2014


'''
d = {1:2,2:3}
h = [1,2,3]
del d[1]
print d
print all(a in d.keys() for a in h)