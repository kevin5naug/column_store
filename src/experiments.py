import os
import sys
import time
import random
import numpy as np
import math
import matplotlib.pyplot as plt
import pickle

create_db = 'create(db,"db1")\n'

def run(script):
    #create script
    f = open("temp.dsl", "w")
    f.write(script)
    f.close()

    #run script
    #Must be in the same dir as client
    start = time.time()
    os.system("./client <temp.dsl")
    end = time.time()
    #os.system("pkill -f './client'")
    return int((end-start) * 1000000) #second to us

def generate_scan_script(pass_num):
    vmin = 0
    vmax = 50000
    dmin = 200
    dmax = 10000
    independent_script = ""
    shared_script = "batch_queries()\n"
    select_template = "s{0}=select(db1.tbl3_batch.col1,{1},{2})\n"
    for i in range(pass_num):
        lower = random.randint(vmin, vmin+dmin)
        upper = random.randint(vmax-dmax, vmax)
        script_unit = select_template.format(i,lower,upper)
        independent_script += script_unit
    shared_script += independent_script
    shared_script += "batch_execute()\n"
    return independent_script, shared_script

def run_shared_scan_experiment(pass_num_max = 1000):
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl3_batch",db1,4)',
            'create(col,"col1",db1.tbl3_batch)',
            'create(col,"col2",db1.tbl3_batch)',
            'create(col,"col3",db1.tbl3_batch)',
            'create(col,"col4",db1.tbl3_batch)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data3_batch.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")
    
    res=dict()
    res['independent']=dict()
    res['shared']=dict()
    for i in range(0, pass_num_max, 5):
        print("Test shared scan for %d queries begins " %(i+1))
        independent_script, shared_script = generate_scan_script(i+1)
        itime = run(independent_script)
        print("can complete independent script")
        stime = run(shared_script)
        print("can complete shared script")
        res['independent'][i+1] = itime
        res['shared'][i+1] = stime

    os.system("pkill -f './server'")

    return res

def run_threaded_scan_experiment(pass_num_max = 1000):
    #enable multithreading
    #initial state set thread num = 4
    #set thread num to 2
    f = open("server.c", "r")
    src=f.read()
    src=src.replace("#define MULTI_THREADING 0", "#define MULTI_THREADING 1")
    src=src.replace("#define THREAD_NUM 4", "#define THREAD_NUM 2")
    f = open("server.c", "w")
    f.write(src)
    f.close()
    print("Multithreading enabled")
    print("Thread num set to 2")
    
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl3_batch",db1,4)',
            'create(col,"col1",db1.tbl3_batch)',
            'create(col,"col2",db1.tbl3_batch)',
            'create(col,"col3",db1.tbl3_batch)',
            'create(col,"col4",db1.tbl3_batch)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data3_batch.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")
    
    #generate scripts
    script_list = []
    for i in range(pass_num_max):
        _, shared_script_unit = generate_scan_script(i+1)
        script_list.append(shared_script_unit)

    res = dict()
    res[2] = dict()
    res[3] = dict()
    res[4] = dict()
    for i in range(len(list(res.keys()))):
        print("Test threaded shared scan for %d thread" % list(res.keys())[i])
        for j in range(0, pass_num_max, 5):
            print("Test threaded shared scan for %d queries begins " %(j+1))
            ttime = run(script_list[j])
            res[list(res.keys())[i]][j+1] = ttime
        os.system("pkill -f './server'")
        
        if i != len(list(res.keys())) - 1:
            #change thread num for next run
            f = open("server.c", "r")
            src = f.read()
            src = src.replace("#define THREAD_NUM %d" % list(res.keys())[i], "#define THREAD_NUM %d" % list(res.keys())[i+1])
            f = open("server.c", "w")
            f.write(src)
            f.close()
        
        setup_script = ""
        setup_script += create_db
        create_and_load = [
                'create(tbl,"tbl3_batch",db1,4)',
                'create(col,"col1",db1.tbl3_batch)',
                'create(col,"col2",db1.tbl3_batch)',
                'create(col,"col3",db1.tbl3_batch)',
                'create(col,"col4",db1.tbl3_batch)',
                'load("/Users/joker/Coding/cs165-2019/test_data/data3_batch.csv")']
        setup_script += "\n".join(create_and_load) + "\n"

        #initial state set multithreading off

        #clean and compile code
        os.system("make distclean > compile.out")
        os.system("make > compile.out")

        #run server
        os.system("./server > server.out &")
        time.sleep(1)
        print("initiate server complete")

        #setup
        print("set up time: ", run(setup_script))
        print("setup db complete")
    
    #no need to set thread num back to 4
    os.system("pkill -f './server'")
    return res

def visualize_shared_scan(d):
    dpath='/Users/joker/Coding/cs165-2019/'
    ind_d=d['independent']
    shared_d=d['shared']
    ind_x=[]
    ind_y=[]
    shared_x=[]
    shared_y=[]
    for key, value in ind_d.items():
        ind_x.append(key)
        ind_y.append(value)
    for key, value in shared_d.items():
        shared_x.append(key)
        shared_y.append(value)
        
    fig=plt.figure()
    plt.plot(ind_x, ind_y, label="independent")
    plt.plot(shared_x, shared_y, label="shared")
    fig.suptitle('independent vs shared scan')
    plt.xlabel('select queries num')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'shared_scan.png')
    
def visualize_threaded_scan(d_shared, d):
    dpath='/Users/joker/Coding/cs165-2019/'
    shared_d=d_shared['shared']
    shared_x=[]
    shared_y=[]
    for key, value in shared_d.items():
        shared_x.append(key)
        shared_y.append(value)
    
    d2=d[2]
    d3=d[3]
    d4=d[4]
    d2_x=[]
    d2_y=[]
    d3_x=[]
    d3_y=[]
    d4_x=[]
    d4_y=[]
    for key, value in d2.items():
        d2_x.append(key)
        d2_y.append(value)
    for key, value in d3.items():
        d3_x.append(key)
        d3_y.append(value)
    for key, value in d4.items():
        d4_x.append(key)
        d4_y.append(value)
        
    fig=plt.figure()
    plt.plot(shared_x, shared_y, label="1 core")
    plt.plot(d2_x, d2_y, label="2 core")
    plt.plot(d3_x, d3_y, label="3 core")
    plt.plot(d4_x, d4_y, label="4 core")
    fig.suptitle('multithreaded shared scan')
    plt.xlabel('select queries num')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'threaded_scan.png')

def generate_select_with_selectivity_script(selectivity, pass_num = 100):
    vmin = 0
    vmax = 100000
    script = ""
    select_template = "s{0}=select(db1.tbl1.col2,{1},{2})\n"
    for i in range(pass_num):
        lower = random.randint(vmin, vmax - int(selectivity*(vmax-vmin))-1)
        upper = lower+int(selectivity*(vmax-vmin))+1
        script_unit = select_template.format(i,lower,upper)
        script += script_unit
    return script

def run_scan_with_selectivity_experiment(trial_num=160):
    selectivity_list=[i/(trial_num/0.8) for i in range(trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for selectivity in selectivity_list:
        print("Test scan with %f selectivity begins " %(selectivity))
        ttime = run(generate_select_with_selectivity_script(selectivity))
        print("Test finish")
        res[selectivity] = ttime
    
    os.system("pkill -f './server'")

    return res

def visualize_scan_with_selectivity(d):
    dpath='/Users/joker/Coding/cs165-2019/'
    x_samples=[]
    y_samples=[]
    for key, value in d.items():
        x_samples.append(key)
        y_samples.append(value)
        
    fig=plt.figure()
    plt.plot(x_samples, y_samples, label="scan")
    fig.suptitle('scan with increased selectivity')
    plt.xlabel('selectivity')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'scan_selectivity.png')

def create_scan_selectivity_test_cases():
    script=generate_select_with_selectivity_script(0.10,pass_num = 10)
    f = open("/Users/joker/Coding/cs165-2019/test_data/low_selectivity.dsl", "w")
    f.write(script)
    f.close()
    
    script=generate_select_with_selectivity_script(0.50,pass_num = 10)
    f = open("/Users/joker/Coding/cs165-2019/test_data/mid_selectivity.dsl", "w")
    f.write(script)
    f.close()
    
    script=generate_select_with_selectivity_script(0.90,pass_num = 10)
    f = open("/Users/joker/Coding/cs165-2019/test_data/high_selectivity.dsl", "w")
    f.write(script)
    f.close()

def dump_result(d, name):
    fpath = "/Users/joker/Coding/cs165-2019/" + name + ".obj"
    f = open(fpath, "wb")
    pickle.dump(d, f)
    f.close()

def visualize_scan_with_and_without_if():
    dpath='/Users/joker/Coding/cs165-2019/'
    fpath=dpath+'scan_selectivity.obj'
    with open(fpath, 'rb') as pickle_file:
        d = pickle.load(pickle_file)
    x_samples_if=[]
    y_samples_if=[]
    for key, value in d.items():
        x_samples_if.append(key)
        y_samples_if.append(value)
    
    fpath=dpath+'scan_selectivity_without_if.obj'
    with open(fpath, 'rb') as pickle_file:
        d = pickle.load(pickle_file)
    x_samples_without_if=[]
    y_samples_without_if=[]
    for key, value in d.items():
        x_samples_without_if.append(key)
        y_samples_without_if.append(value)
    
        
    fig=plt.figure()
    plt.plot(x_samples_if, y_samples_if, label="scan with if")
    plt.plot(x_samples_without_if, y_samples_without_if, label="scan without if")
    fig.suptitle('scan with if vs scan without if')
    plt.xlabel('selectivity')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'scan_selectivity_comparison.png')


def run_clustered_btree_select_with_selectivity_experiment(trial_num=160):
    selectivity_list=[i/(trial_num/0.8) for i in range(trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col2,btree,clustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for selectivity in selectivity_list:
        print("Test scan with %f selectivity begins " %(selectivity))
        ttime = run(generate_select_with_selectivity_script(selectivity))
        print("Test finish")
        res[selectivity] = ttime

    os.system("pkill -f './server'")

    return res

def run_clustered_sort_select_with_selectivity_experiment(trial_num=160):
    selectivity_list=[i/(trial_num/0.8) for i in range(trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col2,sorted,clustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for selectivity in selectivity_list:
        print("Test scan with %f selectivity begins " %(selectivity))
        ttime = run(generate_select_with_selectivity_script(selectivity))
        print("Test finish")
        res[selectivity] = ttime

    os.system("pkill -f './server'")

    return res

def visualize_clustered_index(dscan, dbtree, dsort):
    dpath='/Users/joker/Coding/cs165-2019/'
    x1_samples=[]
    y1_samples=[]
    for key, value in dscan.items():
        x1_samples.append(key)
        y1_samples.append(value)
    
    x2_samples=[]
    y2_samples=[]
    for key, value in dbtree.items():
        x2_samples.append(key)
        y2_samples.append(value)
    
    x3_samples=[]
    y3_samples=[]
    for key, value in dsort.items():
        x3_samples.append(key)
        y3_samples.append(value)
        
    fig=plt.figure()
    plt.plot(x1_samples[1:], y1_samples[1:], label="scan")
    plt.plot(x2_samples[1:], y2_samples[1:], label="btree clustered")
    plt.plot(x3_samples[1:], y3_samples[1:], label="sort clustered")
    fig.suptitle('select with scan, btree clustered and sort clustered')
    plt.xlabel('selectivity')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'select_index_selectivity.png')



def run_unclustered_btree_select_with_selectivity_experiment(trial_num=160):
    selectivity_list=[i/(trial_num/0.8) for i in range(trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col2,btree,unclustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for selectivity in selectivity_list:
        print("Test scan with %f selectivity begins " %(selectivity))
        ttime = run(generate_select_with_selectivity_script(selectivity))
        print("Test finish")
        res[selectivity] = ttime

    os.system("pkill -f './server'")

    return res

def run_unclustered_sort_select_with_selectivity_experiment(trial_num=160):
    selectivity_list=[i/(trial_num/0.8) for i in range(trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col2,sorted,unclustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for selectivity in selectivity_list:
        print("Test scan with %f selectivity begins " %(selectivity))
        ttime = run(generate_select_with_selectivity_script(selectivity))
        print("Test finish")
        res[selectivity] = ttime

    os.system("pkill -f './server'")

    return res

def visualize_unclustered_index(dscan, dbtree, dsort):
    dpath='/Users/joker/Coding/cs165-2019/'
    x1_samples=[]
    y1_samples=[]
    for key, value in dscan.items():
        x1_samples.append(key)
        y1_samples.append(value)
    
    x2_samples=[]
    y2_samples=[]
    for key, value in dbtree.items():
        x2_samples.append(key)
        y2_samples.append(value)
    
    x3_samples=[]
    y3_samples=[]
    for key, value in dsort.items():
        x3_samples.append(key)
        y3_samples.append(value)
        
    fig=plt.figure()
    plt.plot(x1_samples[1:], y1_samples[1:], label="scan")
    plt.plot(x2_samples[1:], y2_samples[1:], label="btree unclustered")
    plt.plot(x3_samples[1:], y3_samples[1:], label="sort unclustered")
    fig.suptitle('select with scan, btree unclustered and sort unclustered')
    plt.xlabel('selectivity')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'select_unclustered_index_selectivity.png')


def generate_nested_loop_and_hash_join_script_hard_reset(size, query_num = 100):
    vmin = 0
    vmax = 100000
    nested_script = ""
    hash_script = ""
    for i in range(query_num):
        lower = random.randint(vmin, vmax - size -1)
        upper = lower+size
        
        nested_setup_lines = [
        "s1{}=select(db1.tbl1.col1,{},{})".format(i, lower, upper),
        "s2{}=select(db1.tbl1.col2,{},{})".format(i, lower, upper),
        "f1{}=fetch(db1.tbl1.col1,s1{})".format(i,i),
        "f2{}=fetch(db1.tbl1.col2,s2{})".format(i,i),
        "p1{0},p2{0}=join(f1{0},s1{0},f2{0},s2{0},nested-loop)\n".format(i)]
        
        hash_setup_lines = [
        "s1{}=select(db1.tbl1.col1,{},{})".format(i, lower, upper),
        "s2{}=select(db1.tbl1.col2,{},{})".format(i, lower, upper),
        "f1{}=fetch(db1.tbl1.col1,s1{})".format(i,i),
        "f2{}=fetch(db1.tbl1.col2,s2{})".format(i,i),
        "p1{0},p2{0}=join(f1{0},s1{0},f2{0},s2{0},hash)\n".format(i)]
        
        
        nested_script_unit = "\n".join(nested_setup_lines) + "\n"
        hash_script_unit = "\n".join(hash_setup_lines) + "\n"
        nested_script += nested_script_unit
        hash_script += hash_script_unit
    return nested_script, hash_script

def generate_nested_loop_and_hash_join_script(size, query_num = 100):
    vmin = 0
    vmax = 100000
    nested_script = ""
    hash_script = ""
    lower = random.randint(vmin, vmax - size -1)
    upper = lower+size
    setup_lines = [
    "s1=select(db1.tbl1.col1,{},{})".format(lower,upper),
    "s2=select(db1.tbl1.col2,{},{})".format(lower,upper),
    "f1=fetch(db1.tbl1.col1,s1)",
    "f2=fetch(db1.tbl1.col2,s2)\n"]
    nested_script += "\n".join(setup_lines)+"\n"
    hash_script += "\n".join(setup_lines) + "\n"
    for i in range(query_num):
        nested_unit = "p1{0},p2{0}=join(f1,s1,f2,s2,nested-loop)\n".format(i)
        hash_unit = "p1{0},p2{0}=join(f1,s1,f2,s2,hash)\n".format(i)
        nested_script += nested_unit
        hash_script += hash_unit
    return nested_script, hash_script


def run_nested_loop_and_hash_join_experiment(trial_num=100):
    data_size_list=[int(i*(10000/trial_num)) for i in range(1, trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    res['nested-loop']=dict()
    res['hash']=dict()
    for data_size in data_size_list:
        print("Test scan with %d data_size begins " %(data_size))
        nested_script, hash_script = generate_nested_loop_and_hash_join_script(data_size)
        ntime = run(nested_script)
        htime = run(hash_script)
        print("Test finish")
        res['nested-loop'][data_size]=ntime
        res['hash'][data_size]=htime

    os.system("pkill -f './server'")

    return res

def visualize_nested_loop_and_hash_join(d):
    dpath='/Users/joker/Coding/cs165-2019/'
    nested_d=d['nested-loop']
    hash_d=d['hash']
    nested_x=[]
    nested_y=[]
    hash_x=[]
    hash_y=[]
    for key, value in nested_d.items():
        nested_x.append(key)
        nested_y.append(value)
    for key, value in hash_d.items():
        hash_x.append(key)
        hash_y.append(value)
        
    fig=plt.figure()
    plt.plot(nested_x[:-1], nested_y[:-1], label="nested-loop join")
    plt.plot(hash_x[:-1], hash_y[:-1], label="hash join")
    fig.suptitle('Nested-loop vs Hash Join')
    plt.xlabel('data size')
    plt.ylabel('time(us)')
    plt.legend(loc='best')
    fig.savefig(dpath+'nested_and_hash_join_comparison.png')


def run_hash_join_experiment(trial_num=100):
    data_size_list=[int(i*(10000/trial_num)) for i in range(1, trial_num)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #initial state set multithreading off

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for data_size in data_size_list:
        print("Test scan with %d data_size begins " %(data_size))
        _, hash_script = generate_nested_loop_and_hash_join_script(data_size)
        htime = run(hash_script)
        print("Test finish")
        res[data_size]=htime

    os.system("pkill -f './server'")

    return res

def visualize_normal_and_grace_hash():
    dpath='/Users/joker/Coding/cs165-2019/'
    fpath=dpath+'normal_hash.obj'
    with open(fpath, 'rb') as pickle_file:
        d = pickle.load(pickle_file)
    x_samples_normal=[]
    y_samples_normal=[]
    for key, value in d.items():
        x_samples_normal.append(key)
        y_samples_normal.append(value)

    fpath=dpath+'grace_hash.obj'
    with open(fpath, 'rb') as pickle_file:
        d = pickle.load(pickle_file)
    x_samples_grace=[]
    y_samples_grace=[]
    for key, value in d.items():
        x_samples_grace.append(key)
        y_samples_grace.append(value)

        
    fig=plt.figure()
    plt.plot(x_samples_normal[:-1], y_samples_normal[:-1], label="normal hash join")
    plt.plot(x_samples_grace[:-1], y_samples_grace[:-1], label="grace hash join")
    fig.suptitle('normal hash join vs grace hash join')
    plt.xlabel('data size')
    plt.ylabel('time(ms)')
    plt.legend(loc='best')
    fig.savefig(dpath+'normal_hash_and_grace_hash_comparison.png')


def generate_insert_script(query_size):
    vmin = 0
    vmax = 100000
    script = ""
    insert_template = "relational_insert(db1.tbl1,{0},{1})\n"
    for i in range(query_size):
        first = random.randint(vmin, vmax-1)
        second = random.randint(vmin, vmax-1)
        script_unit = insert_template.format(first, second)
        script += script_unit
    return script

def run_insert_without_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime
    
    os.system("pkill -f './server'")

    return res
    
def run_insert_with_single_btree_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col1,btree,clustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime

    os.system("pkill -f './server'")

    return res

def run_insert_with_double_btree_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col1,btree,clustered)',
            'create(idx,db1.tbl1.col2,btree,unclustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime

    os.system("pkill -f './server'")

    return res

def visualize_clustered_index(d0, d1, d2):
    dpath='/Users/joker/Coding/cs165-2019/'
    x1_samples=[]
    y1_samples=[]
    for key, value in d0.items():
        x1_samples.append(key)
        y1_samples.append(value)

    x2_samples=[]
    y2_samples=[]
    for key, value in d1.items():
        x2_samples.append(key)
        y2_samples.append(value)

    x3_samples=[]
    y3_samples=[]
    for key, value in d2.items():
        x3_samples.append(key)
        y3_samples.append(value)
        
    fig=plt.figure()
    plt.plot(x1_samples[1:], y1_samples[1:], label="no index")
    plt.plot(x2_samples[1:], y2_samples[1:], label="1 btree index")
    plt.plot(x3_samples[1:], y3_samples[1:], label="2 btree index")
    fig.suptitle('additional cost to insert into table with indexes')
    plt.xlabel('insert query num')
    plt.ylabel('time(ms)')
    plt.legend(loc='best')
    fig.savefig(dpath+'additional_cost_comparison.png')



def run_insert_with_single_sorted_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col1,sorted,clustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime

    os.system("pkill -f './server'")

    return res

def run_insert_with_double_sorted_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col1,sorted,clustered)',
            'create(idx,db1.tbl1.col2,sorted,unclustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime

    os.system("pkill -f './server'")

    return res
    
def run_insert_with_sorted_btree_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col1,sorted,clustered)',
            'create(idx,db1.tbl1.col2,btree,unclustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime

    os.system("pkill -f './server'")

    return res

def run_insert_with_btree_sorted_index_experiment(max_query_size=100):
    query_size_list = [i for i in range(1, max_query_size, 5)]
    setup_script = ""
    setup_script += create_db
    create_and_load = [
            'create(tbl,"tbl1",db1,2)',
            'create(col,"col1",db1.tbl1)',
            'create(col,"col2",db1.tbl1)',
            'create(idx,db1.tbl1.col1,btree,clustered)',
            'create(idx,db1.tbl1.col2,sorted,unclustered)',
            'load("/Users/joker/Coding/cs165-2019/test_data/data_for_selectivity.csv")']
    setup_script += "\n".join(create_and_load) + "\n"

    #clean and compile code
    os.system("make distclean > compile.out")
    os.system("make > compile.out")

    #run server
    os.system("./server > server.out &")
    time.sleep(1)
    print("initiate server complete")

    #setup
    print("set up time: ", run(setup_script))
    print("setup db complete")

    res=dict()
    for query_size in query_size_list:
        print("Test insert without index with %d query size begins " %(query_size))
        ttime = run(generate_insert_script(query_size))
        print("Test finish")
        res[query_size] = ttime

    os.system("pkill -f './server'")

    return res
    

def visualize_all_index(d0, d1, d2, d3, d4, d5, d6):
    dpath='/Users/joker/Coding/cs165-2019/'
    x1_samples=[]
    y1_samples=[]
    for key, value in d0.items():
        x1_samples.append(key)
        y1_samples.append(value)

    x2_samples=[]
    y2_samples=[]
    for key, value in d1.items():
        x2_samples.append(key)
        y2_samples.append(value)

    x3_samples=[]
    y3_samples=[]
    for key, value in d2.items():
        x3_samples.append(key)
        y3_samples.append(value)
    
    x4_samples=[]
    y4_samples=[]
    for key, value in d3.items():
        x4_samples.append(key)
        y4_samples.append(value)

    x5_samples=[]
    y5_samples=[]
    for key, value in d4.items():
        x5_samples.append(key)
        y5_samples.append(value)

    x6_samples=[]
    y6_samples=[]
    for key, value in d5.items():
        x6_samples.append(key)
        y6_samples.append(value)
        
    x7_samples=[]
    y7_samples=[]
    for key, value in d6.items():
        x7_samples.append(key)
        y7_samples.append(value)
        
    fig=plt.figure()
    plt.plot(x1_samples[1:], y1_samples[1:], label="no index")
    plt.plot(x2_samples[1:], y2_samples[1:], label="1 btree index")
    plt.plot(x3_samples[1:], y3_samples[1:], label="2 btree index")
    plt.plot(x4_samples[1:], y4_samples[1:], label="1 sorted index")
    plt.plot(x5_samples[1:], y5_samples[1:], label="2 sorted index")
    plt.plot(x6_samples[1:], y6_samples[1:], label="sort & btree index")
    plt.plot(x7_samples[1:], y7_samples[1:], label="btree & sort index")
    fig.suptitle('additional cost to insert into table with indexes')
    plt.xlabel('insert query num')
    plt.ylabel('time(ms)')
    plt.legend(loc='best')
    fig.savefig(dpath+'full_additional_cost_comparison.png')

#shared scan

#res1=run_shared_scan_experiment(pass_num_max = 100)
#res2=run_threaded_scan_experiment(pass_num_max = 100)
#visualize_shared_scan(res1)
#visualize_threaded_scan(res1, res2)


#scan with varying selectivity

#res=run_scan_with_selectivity_experiment()
#dump_result(res, "scan_selectivity")
#visualize_scan_with_selectivity(res)
#create_scan_selectivity_test_cases()
#visualize_scan_with_and_without_if()


#clustered index

#res1=run_scan_with_selectivity_experiment();
#res2=run_clustered_btree_select_with_selectivity_experiment();
#res3=run_clustered_sort_select_with_selectivity_experiment();
#visualize_clustered_index(res1, res2, res3)


#unclustered index

#res1=run_scan_with_selectivity_experiment();
#res2=run_unclustered_btree_select_with_selectivity_experiment();
#res3=run_unclustered_sort_select_with_selectivity_experiment();
#visualize_unclustered_index(res1, res2, res3)


#nested-loop join vs hash join

#res=run_nested_loop_and_hash_join_experiment(trial_num=10)
#visualize_nested_loop_and_hash_join(res)



#hash join vs grace hash join

#hash_res=run_hash_join_experiment(trial_num=10)
#dump_result(hash_res, "grace_hash")
#visualize_normal_and_grace_hash()



#additional cost to insert into table with indexes
d0=run_insert_without_index_experiment(max_query_size=200)
d1=run_insert_with_single_btree_index_experiment(max_query_size=200)
d2=run_insert_with_double_btree_index_experiment(max_query_size=200)
visualize_clustered_index(d0, d1, d2)

d3=run_insert_with_single_sorted_index_experiment(max_query_size=200)
d4=run_insert_with_double_sorted_index_experiment(max_query_size=200)
d5=run_insert_with_sorted_btree_index_experiment(max_query_size=200)
d6=run_insert_with_btree_sorted_index_experiment(max_query_size=200)
visualize_all_index(d0, d1, d2, d3, d4, d5, d6)

