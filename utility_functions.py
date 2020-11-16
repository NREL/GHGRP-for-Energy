# -*- coding: utf-8 -*-
"""
Created on Mon May 11 18:08:07 2020

@author: KPE

This is a program that has python utility functions in it. 
First is a function that finds the unique values from a list. The second will write a temporary file.
"""
#
# the following function takes a list and finds unique values in the list
#
def find_unique_vals(listx):
    unique_listx = []
    listx_sort = []
    listx_sort = sorted(listx)
    #
  #  print(unique_listx[0]) 
#    print(listx_sort[0])
    # appends the first element no matter what, because this will 
    unique_listx.extend(listx_sort[0])
    iter = 1
    while iter <= len(listx_sort):
        if listx_sort[iter] != listx_sort[iter-1]:
            unique_listx.extend(listx_sort[iter])
        iter = iter+1
    return unique_listx

# the following function takes a list, writes a json file, and empties the list
    
def interim_jdump(listx,tempfilex):
    import json
    with open (tempfilex, "w") as writesub:
        json.dump(listx, writesub)
    # now clear the list
    listx = []
    return listx
# 
# this function gets a count of a url file
#
def get_ghgrp_obs(urlct): 
    import requests
    count2 = []
    count3 = []
    # now for the first request!  variables from ghgrp
    # print(ghgrp_init + ghgrp_spfa + ghgrp_penu + ghgrp_ct)
    count1 = requests.get(urlct)
#    print(count1.text)
    count2 = count1.text.split(">")
#    print(count2)
    for zilch in count2:
        snakeeyes = zilch.split("<")
        count3.extend(snakeeyes)
    obs = int(count3[6])
    print ("Number of observations in " + urlct + ": " + str(obs))
    return obs


# this is a ghgrp write file function. Arguments:  List to be written, 
# subpart, file name, and ~ last observation, (=0 if full file write)   
def create_ghgrp_writfl(the_listx,subpt,zed,endz):
    import json
    #
    # the_subp is the name of the written list. 
    if subpt != "PUB":
        the_subp = "Subpart_" + zed[1:]
        the_write_file =  the_subp +"_" + str(endz) + ".json"
    elif zed[1:11] != "PUB_DIM_FAC":  # for the pub files, which are slightly different
        the_subp = zed[5:len(zed)] 
        the_write_file =  the_subp +"_" + str(endz) + ".json"
    else:   # if we have pub_dim facility
        the_subp = zed
        the_write_file =  the_subp + ".json"
    # now let's write the file
    print("The write file will be " + the_write_file)
    with open (the_write_file, "w") as writesub:
        json.dump(the_listx, writesub)
    the_listx = []  # empty this list
    return the_listx

# do a pandas write for this one
#  inputs:  pandas dataframe, directory path, subpt, "zed" = content of file, endz = end observation    
#
def pickled_pandas(dfx,pathx,subpt,zed,endz):
 #   import json
    import pandas as pd 
    #
    # the_subp is the name of the written list. 
    if subpt != "PUB":
        the_subp = "Subpart_" + zed[1:]
    else:   # if we have pub_ file
        the_subp = zed
    pandax_name =   pathx +  the_subp +"_" + str(endz) + ".uncm"   #  bs extension for uncompressed data file.  
    # now let's write the file
    print("The write file will be " + pandax_name)
#   
    dfx.to_pickle(pandax_name, compression=None)
    return  # pandax_name
#
#  pickle pandas with compression & no subparts
def pickled_pandas_2(thepanda,pathx,filexo):
 #   import json
    import pandas as pd 
    #
    pandax_name =   pathx +  filexo + ".gzip"   #  bs extension for uncompressed data file.  
    # now let's write the file
    print("The write file will be " + pandax_name)
#   
    thepanda.to_pickle(pandax_name)
    return  # pandax_name


    



# this is a ghgrp write file function. Arguments:  List to be written, 
# subpart, file name, and ~ last observation, (=0 if full file write)   
def create_ghgrp_pickle(the_listx,subpt,zed,endz):
    import pandas as pd
    import json
    #
    # the_subp is the name of the written list. 
    if subpt != "PUB":
        the_subp = "Subpart_" + zed[1:]
        the_write_file =  the_subp +"_" + str(endz) + ".gzip"
    elif zed[:10] != "PUB_DIM_FAC":  # for the pub files, which are slightly different
        the_subp = zed[5:len(zed)] 
        the_write_file =  the_subp +"_" + str(endz) + ".gzip"
    else:   # if we have pub_dim facility
        the_subp = zed
        the_write_file =  the_subp + ".gzip"
    # now let's write the file
    print("The write file will be " + the_write_file)
    df = data
#    with open (the_write_file, "w") as writesub:
#        gzip.dump(the_listx, writesub)
    the_listx = []  # empty this list
    return the_listx

    

def get_keys(in_file, incrx):
    # imports
    import json
    import math 
    # initialize
    dataz = []
    obsx = 0
#    incrx = 5000
    with open (in_file,'r') as infilex:
        dataz = json.load(infilex)
    while obsx <= 10*incrx:
            for zoip in dataz:
                print(type(zoip))
#                keyz = zoip.keys()
#                print("Keys for ", infilex, " observation ", obsx, "are:  ",  zoip.keys())
                obsx += incrx
    return
#   this function takes a panda and a list of variables, 
#   reduced the dataset to columns in the list of variables
#   then finds unique values 
#   with the unique values from list of variables (colistx)
#   the return list can be merged in with a pre-existing panda
#  this is useful if you need facilities that exibit behavior in some
#  years but not others    
#    
    
def find_unique(old_panda,colistx):  # this returns a dataframe, but can also write to a file
    # imports
    import pandas as pd
    retlst = []
    # uniquelst = []
    #`
    Finished = False
    try:
      #  print(colistx)
        new_pandaxyz = old_panda.loc[:,colistx]
        new_pandaxxy = new_pandaxyz.drop_duplicates()
        new_pandaxx = new_pandaxxy.sort_values(colistx) # (axis=1,labels=colistx)
        new_pandax = new_pandaxx.reset_index()
        new_panda = new_pandax.drop(columns=['index'])
        print("first lines of observation file", new_panda.head())
        print("columns of observation file",new_panda.columns)
#
        Finished = True
        retlst = [Finished, new_panda]        
    except KeyError as keyz:
        raise KeyError from keyz
        retlst = [Finished, 0]
    except Exception:
        retlst = [Finished, 0]
    return retlst

