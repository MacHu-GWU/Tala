##encoding=utf8

"""
This module provides high performance iterator recipes.
best time and memory complexity implementation applied.

compatible: python2 and python3

import:
    from angora.DATA.iterable import flatten, flatten_all, nth, shuffled, grouper, grouper_dict, grouper_list
    from angora.DATA.iterable import running_windows, cycle_running_windows, cycle_slice
"""

from __future__ import print_function
import collections
import itertools
import random
import sys

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    from itertools import ifilterfalse as filterfalse, izip_longest as zip_longest
else: # in python3
    from itertools import filterfalse, zip_longest
    
def flatten(listOfLists):
    "Flatten one level of nesting"
    return itertools.chain.from_iterable(listOfLists)

def flatten_all(listOfLists):
    "Flatten arbitrary depth of nesting"
    for i in listOfLists:
        if hasattr(i, "__iter__"):
            for j in flatten_all(i):
                yield j
        else:
            yield i

def nth(iterable, n, default=None):
    "Returns the nth item or a default value"
    return next(itertools.islice(iterable, n, None), default)

def shuffled(iterable):
    "Returns the shuffled iterable"
    return random.sample(iterable, len(iterable))

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

def grouper_dict(DICT, n):
    "evenly divide DICTIONARY into fixed-length piece, no filled value if chunk size smaller than fixed-length"
    for group in grouper(DICT, n):
        chunk_d = dict()
        for k in group:
            if k != None:
                chunk_d[k] = DICT[k]
        yield chunk_d

def grouper_list(LIST, n):
    "evenly divide LIST into fixed-length piece, no filled value if chunk size smaller than fixed-length"
    for group in grouper(LIST, n):
        chunk_l = list()
        for i in group:
            if i != None:
                chunk_l.append(i)
        yield chunk_l

def running_windows(iterable, size):
    """generate n-size running windows
    e.g. iterable = [1,2,3,4,5], size = 3
    yield: [1,2,3], [2,3,4], [3,4,5]
    """
    fifo = collections.deque(maxlen=size)
    for i in iterable:
        fifo.append(i)
        if len(fifo) == size:
            yield list(fifo)
            
def cycle_running_windows(iterable, size):
    """generate n-size cycle running windows
    e.g. iterable = [1,2,3,4,5], size = 2
    yield: [1,2], [2,3], [3,4], [4,5], [5,1]
    """
    fifo = collections.deque(maxlen=size)
    cycle = itertools.cycle(iterable)
    counter = itertools.count(1)
    length = len(iterable)
    for i in cycle:
        fifo.append(i)
        if len(fifo) == size:
            yield list(fifo)
            if next(counter) == length:
                break

def cycle_slice(LIST, start, end): # 测试阶段, 不实用
    """given a list, return right hand cycle direction slice from start to end
    e.g.
        array = [0,1,2,3,4,5,6,7,8,9]
        cycle_slice(array, 4, 7) -> [4,5,6,7]
        cycle_slice(array, 8, 2) -> [8,9,0,1,2]
    """
    if type(LIST) != list:
        LIST = list(LIST)
        
    if end >= start:
        return LIST[start:end+1]
    else:
        return LIST[start:] + LIST[:end+1]
                
if __name__ == "__main__":
    from angora.GADGET import timetest
    import time
    def test_flatten():
        """测试flatten的性能
        """
        print("{:=^40}".format("test_flatten"))
        complexity = 1000
        a = [[1,2,3],[4,5,6],[7,8,9,10]] * complexity
        b = range(10 * complexity)
        
        st = time.clock()
        for _ in flatten(a):
            pass
        print(time.clock() - st)
        
        st = time.clock()
        for _ in b:
            pass
        print(time.clock() - st)
    
#     test_flatten()
    
    def test_flatten_all():
        """测试flatten_all的性能
        """
        print("{:=^40}".format("test_flatten_all"))
        complexity = 1000
        a = [[1,2,3],[4,[5,6],[7,8]], [9,10]] * complexity
        b = range(complexity * 10)
        st = time.clock()
        for _ in flatten_all(a):
            pass
        print(time.clock() - st)
        
        st = time.clock()
        for _ in b:
            pass
        print(time.clock() - st)
        
#     test_flatten_all()

    def test_nth():
        """测试nth的性能
        """
        print("{:=^40}".format("test_flatten_all"))
        n = 10000
        array = [i for i in range(n)]
        
        st = time.clock()
        for i in range(n):
            _ = array[i]
        print(time.clock() - st)
        
        st = time.clock()
        for i in range(n):
            _ = nth(array, i)
        print(time.clock() - st)
        
        st = time.clock()
        for i in array:
            _ = i
        print(time.clock() - st)
        
#     test_nth()

    def test_grouper():
        """Test for grouper, grouper_list, grouper_dict
        """
        print("{:=^40}".format("test_grouper"))
        for chunk in grouper("abcdefg",3):
            print(chunk)
            
#     test_grouper()
    
    def test_grouper_dict_list():
        """Test for grouper_dict, grouper_list
        """
        print("{:=^40}".format("test_grouper_dict_list"))
        print("=== test for grouper_dict ===")
        a = {key: "hello" for key in range(10)} ## test grouper_list
        for chunk_d in grouper_dict(a, 3):
            print(chunk_d)
            
        print("=== test for grouper_list ===")
        b = range(10) # test grouper_dict
        for chunk_l in grouper_list(b, 3):
            print(chunk_l)
            
#     test_grouper_dict_list()

    def timetest_grouper():
        array = [[1,2,3] for _ in range(1000)]
        
        def regular():
            for item in array:
                pass
            
        def use_grouper():
            for chunk_l in grouper_list(array, 10):
                for item in chunk_l:
                    pass
                
        timetest(regular, 1000)
        timetest(use_grouper, 1000)
        
#     timetest_grouper()
    
    def test_running_windows():
        print("{:=^40}".format("test_running_windows"))
        array = [0,1,2,3,4]
        
        print("Testing running windows")
        for i in running_windows(array,3): # 测试 窗宽 = 3
            print(i)
        for i in running_windows(array, 1): # 测试 窗宽 = 1
            print(i)
        for i in running_windows(array, 0): # 测试 窗宽 = 0
            print(i)
        print("Testing cycle running windows")
        for i in cycle_running_windows(array, 3): # 测试 窗宽 = 3
            print(i)
        for i in cycle_running_windows(array, 1): # 测试 窗宽 = 1
            print(i)
        for i in cycle_running_windows(array, 0): # 测试 窗宽 = 0
            print(i)
            
#     test_running_windows()
            
    def test_cycle_slice():
        print("{:=^40}".format("test_cycle_slice"))
        array = [0,1,2,3,4,5,6,7,8,9]
        
        print("Testing cycle slice")
        print(cycle_slice(array, 3, 6) )
        print(cycle_slice(array, 6, 3) )
        
#     test_cycle_slice()