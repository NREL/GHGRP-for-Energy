# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 13:47:11 2020

@author: KPE
"""#
# the latest has been moved here from ghgru_functions.py this is the latest version
#
#
def intake_ghgrp_PUB_CK(): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
    import json
    from ghgrp_list_func import pubdimfacility   # the subpart functions
#    import math
    # reset input list and number of observations
    datay = []    # data download file
#    datafullx = []  # full data file that gets written to json
    obsx = 0
    nodictobsx = 0
    dictobsx = 0
    # need to build a loop here with the appropriate "pub" file.
    for ix in range(3):
        file_o = pubdimfacility(ix)
        file_in = "L:/mid/kpe/data/api_scraping/ghgrp_write_files/" + file_o
        print (file_in)
        with open(file_in, 'r') as innerx:
            datay = json.load(innerx)
            # start file loop through dictionariesjs
            for dictox in datay:
                if (obsx % 100) == 0:
                    print("Type of data record, variable, # of dictionary obs & # non-dictionary observaions: ", type(dictox), dictobsx, nodictobsx)
                if type(dictox) != dict:
                    if nodictobsx == 0:
                        nodictobsx = 1
                    if nodictobsx <= 5:
                        print("Data type is ", type(datay), " loop object type is ", type(dictox), "object is: ", dictox, " observation is ", obsx)
                        print("Loop obervation is ", nodictobsx)
                    nodictobsx += 1  
                else:
                    if dictobsx == 0:
                        dictobsx = 1
                    dictobsx +=1
                obsx += 1
            datay = []  # this won't cut if we want to write a file
        print("Dictionary observations:  ", dictobsx, "Non-dictionary observations: ", nodictobsx )
        print("Values reset.")
        dictobsx = 0
        nodictobsx = 0
#    with open(file_out, 'w') as file_outx:
#        json.dump(datafullx, file_outx)
    return obsx

def intake_ghgrp_PUB_con(file_dir): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
    import json
    from ghgrp_list_func import subpartW_15 as subptf    # the subpart functions
    from data_clean_fns import data_pandasB as panwrite # (in_list,out_filex)
    # file_dir = "L:/mid/kpe/data/api_scraping/ghgrp_write_files/" 
    # reset input list and number of observations
    datay = []    # data download file
    obsx = 0
    nodictobsx = 0
    dictobsx = 0
    outlst = []
    file_tx = []
     # need to build a loop here with the appropriate "pub" file.
    for ix in range(11,12):  # starting with one file to test - value should be range(8); looping works well k
        # file input and output names
        file_inx = subptf(ix) # pubdimfacility(ix)
        file_in = file_dir + file_inx   # file_dir was "L:/mid/kpe/data/api_scraping/ghgrp_write_files/" 
        # now create an outfile for this thing from the infile name
        file_tx = file_inx.split(".")
        file_o = file_tx[0]
        file_outx = file_dir + file_o + ".gzip"
        print ("Input file is:  ", file_in)
        # end file input and outpu"t namessu
        with open(file_in, 'r') as innerx:
            datay = json.load(innerx)
            # start file loop through dictionaries
            for dictox in datay:
#                print(dictox)
                if type(dictox) != dict:
                    if nodictobsx <= 5:
                        print("Data type is ", type(datay), " loop object type is ", type(dictox), "object is: ", dictox, " observation is ", obsx)
                        print("Loop obervation is ", nodictobsx)
                    nodictobsx += 1  
                else:  # if the observation is a dictionary, write it to a list
                    outlst.insert(0,dictox)   # .extend didn't work. maybe insert will.
#                    if dictobsx % 100 == 1:
#                        print("Dictionary and last elemet of list", dictox, outlst[0])
#                        print("Facility ID value:  ", dictox["FACILITY_ID"])
#                        print("Dictox element is:  ", dictox)
                    dictobsx +=1
                obsx += 1
            datay = []  # this is OK because outlist is the list we use to write
        print("Dictionary observations:  ", dictobsx, "Non-dictionary observations: ", nodictobsx )
        panwrite(outlst,file_outx)
        print("Values reset.")
        dictobsx = 0
        nodictobsx = 0
        outlst = []
        file_tx = []
    return obsx
#
#
#
# now let's append the pub file and do frequencies

def intake_ghgrp_PUB_ff(file_dir,outpickle): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
    from ghgrp_list_func import pubdimfacility   # the subpart functions
#    from data_clean_fns import data_pandasB as panwrite # (in_list,out_filex)
    import pandas as pd
    import math 
    file_tx = []
    dflstx = []
    serix = []
    setlistx = []
    typedictx = {}
    typedictx = {"CITY": "str", "STATE": "str", "ZIP": "int64",   
                 "COUNTY_FIPS": "int64", "COUNTY": "str", "ADDRESS1": "str", "ADDRESS2": "str", "FACILITY_NAME": "str", "STATE_NAME": "str",
                 "NAICS_CODE": "int64", "BAMM_USED_DESC": "str", "EMISSION_CLASSIFICATION_CODE": "str", "PROGRAM_NAME": "str",
                 "CEMS_USED": "str", "CO2_CAPTURED": "str", "REPORTED_SUBPARTS": "str",
                 "BAMM_APPROVED": "str", "EMITTED_CO2_SUPPLIED": "str", 
                 "PUBLIC_HTML": "str", "PUBLIC_XML": "str", "LOCATION": "str", "PARENT_COMPANY": "str", "PUBLIC_XML_XML": "str",
                 "REPORTED_INDUSTRY_TYPES": "str", "FACILITY_TYPES": "str", "SUBMISSION_ID": "int64",  "UU_RD_EXEMPT": "str", "REPORTING_STATUS": "str",
                 "PROCESS_STATIONARY_CML": "str", "COMMENTS": "str", "RR_MRV_PLAN_URL": "str", "RR_MONITORING_PLAN": "str", "RR_MONITORING_PLAN_FILENAME": "str"}
     # need to build a loop here with the appropriate "pub" file.
    for ix in range(3):  # starting with one file to test - value should be range(3); looping works well k
        # file input and output names
        file_inx = pubdimfacility(ix)
        file_in = file_dir + file_inx   # file_dir was "L:/mid/kpe/data/api_scraping/ghgrp_write_files/" 
        # now create an outfile for this thing from the infile name
        file_tx = file_inx.split(".")
        file_o = file_tx[0]
        file_outx = file_dir + file_o + ".gzip"  # for this function, this will be the input file
        out_pickle = file_dir + outpickle
        print (file_outx)
        print("Loop value is:  ", ix)
        # end file input and outpu"t names
        # replace this with unpickling
        dflstx.insert(ix,pd.read_pickle(file_outx))
        print("length of data list is:  ", len(dflstx))
    fullpuby = pd.concat(dflstx)
    dattypex = fullpuby.dtypes
    # print("Datatypes of fullpubz:  ", dattypex)
    # convert to datatypes
    fullpubz = fullpuby.fillna(-9)  # allows missing values to be coded so we don't throw an error
    fullpubzx = fullpubz.astype(dtype=typedictx)
    dattypex = fullpubzx.dtypes
    # print("Datatypes of fullpubx:  ", dattypex)
    # now convert NAICS_CODE to integer
    # pd.to_numeric(fullpubx["NAICS_CODE"], downcast="integer")
    # get rid of unwanted NAICS codes --- anything non-industrial
    # we will get industrial codes we need. First, we get rid of utilities, then the non-industrial manufacturing in 2 steps
    #fullpubxx = fullpubzx[fullpubzx["NAICS_CODE"] > 221330]   # [fullpubx["NAICS_CODE"] < 400000]
    fullpubxxy = fullpubzx[fullpubzx["NAICS_CODE"] < 400000] 
    
    # now we get rid of utilities on teh lower end
    # fullpubxy = fullpubzx[fullpubzx["NAICS_CODE"] < 221111] 
    # setlistx = [fullpubxxy, fullpubxy]
    # fullpubxxx =  pd.concat(setlistx)
    fullpubx = fullpubxxy.sort_values(by=["FACILITY_ID", "YEAR"])
    # reindex fullpub
    fullpubzz = fullpubx.reset_index()
    # rename year to reporting year for consistency with other series 
    fullpubzz.rename(columns={"YEAR": "REPORTING_YEAR"}, inplace=True)
    # check on basics 
    #
    # do crosstabs on full pub, but write fullpubs
    # 
    fullpub = fullpubzz.assign(reported_subpart_list = fullpubzz["REPORTED_SUBPARTS"].str.split(","))
    #
    nocolsx = len(fullpubzz.columns)
    colsx = fullpubzz.columns  #  columns (like dictionary keys)
    #
    #
    print("Crosstabs:  each column by reporting year \n")
    for ix in colsx:
        print(pd.crosstab(fullpubzz[ix], fullpubzz["REPORTING_YEAR"], dropna= False))
    # writing full dataset, including that last list variable
    colsx = fullpub.columns  #  columns (like dictionary keys)
    nocolsx = len(fullpub.columns)
    headlstx = fullpub.head(nocolsx)
    tailstx = fullpub.tail(nocolsx)
    dattypex = fullpub.dtypes
    print("DataFrame columns:  \n", colsx, "; DataFrame head:  \n", headlstx, "DataFrame tail:  \n", tailstx, "; datatypes:  \n", dattypex, "\n")


    fullpub.to_pickle(out_pickle)
    return 
#
#
def intake_ghgrp_subp_ff(file_dir,outpickle): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
    from ghgrp_list_func import subpartW_15   # the subpart functions
#    from data_clean_fns import data_pandasB as panwrite # (in_list,out_filex)
    import pandas as pd
    import math 
    file_tx = []
    dflstx = []
    serix = []
    setlistx = []
    typedictx = {}
    typedictx = {"REPORTING_YEAR": "int64", "FACILITY_ID": "int64"}
    for ix in range(11,12):  # starting with one file to test - value should be range(3); looping works well k
        # file input and output names
        file_inx = subpartW_15(ix)
#        file_in = file_dir + file_inx   # file_dir was "L:/mid/kpe/data/api_scraping/ghgrp_write_files/" 
        # now create an outfile for this thing from the infile name
        file_tx = file_inx.split(".")
        file_o = file_tx[0]
        file_outx = file_dir + file_o + ".gzip"  # for this function, this will be the input file
        out_pickle = file_dir + outpickle
        # print (file_outx)
        # print("Loop value is:  ", ix)
        # end file input and outpu"t names
        # replace this with unpickling
        dflstx.insert(0,)
        print("length of data list is:  ", len(dflstx))
    fullpuby = pd.concat(dflstx)
    dattypex = fullpuby.dtypes
    print("Datatypes of fullpubz:  ", dattypex)
    # convert to datatypes
    fullpubz = fullpuby.fillna(-9)  # allows missing values to be coded so we don't throw an error
    fullpubzx = fullpubz.astype(dtype=typedictx)
    dattypex = fullpubzx.dtypes
    print("Datatypes of fullpubx:  ", dattypex)
    # now convert NAICS_CODE to integer
    # pd.to_numeric(fullpubx["NAICS_CODE"], downcast="integer")
    # get rid of unwanted NAICS codes --- anything non-industrial
    # we will get industrial codes we need. First, we get rid of utilities, then the non-industrial manufacturing in 2 steps
    fullpubx = fullpubzx.sort_values(by=["FACILITY_ID", "REPORTING_YEAR"])
    # reindex fullpub
    fullpub = fullpubx.reset_index()
    # rename year to reporting year for consistency with other series 
    # check on basics 
    nocolsx = len(fullpub.columns)
    colsx = fullpub.columns  #  columns (like dictionary keys)
    headlstx = fullpub.head(nocolsx)
    tailstx = fullpub.tail(nocolsx)
    dattypex = fullpub.dtypes
    print("DataFrame columns:  ", colsx, "; DataFrame head:  ", headlstx, "; DataFrame tail:  ", tailstx, "; datatypes:  ", dattypex)
    #
    #
    print("Crosstabs:  each column by reporting year")
    for ix in colsx:
        print(pd.crosstab(fullpub[ix], fullpub["REPORTING_YEAR"], dropna= False))


    fullpub.to_pickle(out_pickle)
    return 
#    
# following function is like ..._ff, but designed for a different file
#    

def intake_ghgrp_PUB_FACTS(file_dir,outpickle): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
    from ghgrp_list_func import pubfacts   # the subpart functions
#    from data_clean_fns import data_pandasB as panwrite # (in_list,out_filex)
    import pandas as pd
    import math 
    file_tx = []
    dflstx = []
    serix = []
    setlistx = []
    typedictx = {}
    typedictx = {"GAS_ID": "int64", "CO2E_EMISSION":"float64"}  # all other types are int64 and OK.
     # need to build a loop here with the appropriate "pub" file.
    for ix in range(8):  # starting with one file to test - value should be range(3); looping works well k
        # file input and output names
        file_inx = pubfacts(ix)
        file_in = file_dir + file_inx   # file_dir was "L:/mid/kpe/data/api_scraping/ghgrp_write_files/" 
        # now create an outfile for this thing from the infile name
        file_tx = file_inx.split(".")
        file_o = file_tx[0]
        file_outx = file_dir + file_o + ".gzip"  # for this function, this will be the input file
        out_pickle = file_dir + outpickle
        print (file_outx)
        print("Loop value is:  ", ix)
        # end file input and outpu"t names
        # replace this with unpickling
        dflstx.insert(-1,pd.read_pickle(file_outx))
        print("length of data list is:  ", len(dflstx))
    fullpuby = pd.concat(dflstx)
#    dattypex = fullpuby.dtypes
#    print("Datatypes of fullpubz:  ", dattypex)
    # convert to datatypes
    fullpubz = fullpuby.fillna(-9)  # allows missing values to be coded so we don't throw an error
    fullpubzx = fullpubz.astype(dtype=typedictx)
    dattypex = fullpubzx.dtypes
    print("Datatypes of fullpubzx:  ", dattypex)
    fullpubx = fullpubzx.sort_values(by=["FACILITY_ID", "YEAR"])
    # reindex fullpub
    fullpub = fullpubx.reset_index()
    # rename year to reporting year for consistency with other series 
    fullpub.rename(columns={"YEAR": "REPORTING_YEAR"}, inplace=True)
    # check on basics 
    nocolsx = len(fullpub.columns)
    colsx = fullpub.columns  #  columns (like dictionary keys)
    headlstx = fullpub.head(nocolsx)
    tailstx = fullpub.tail(nocolsx)
    dattypex = fullpub.dtypes
    print("DataFrame columns:  \n", colsx, "\n DataFrame head:  \n", headlstx, "\n DataFrame tail:  \n", tailstx, "\n Datatypes:  \n", dattypex)
    #
    #
    print("Crosstab 4:  each column by reporting year")
    for ix in colsx:
        print(pd.crosstab(fullpub[ix], fullpub["REPORTING_YEAR"], dropna= False))


    fullpub.to_pickle(out_pickle)
    return 
#
#
# the following procedure sorts the "all" file and adds 2 through 5 digit NAICS codes 
# to the datafile    
# inputs --- datafile directory, input file (no ext) & output file (no ext)    
#
def data_3NAICS(inddir,infilex,outfilex):
    # imports
    import pandas as pd
    import math
#    from data_clean_fns import naicsdigits as getnaics
    # serix = []
    dfnx = []
    # varxlst = []
    # data inputs
    infilexx = inddir + infilex # + ".gzip"
    out_pickle = inddir + outfilex # + ".gzip"
    # sort the files
    infilexy = pd.read_pickle(infilexx)
    infileyy = infilexy.sort_values(by=["FACILITY_ID", "REPORTING_YEAR"])
    # new_index = [range(len(infiley))]
    #
    infiley = infileyy.reset_index(drop=True)
    nocolsx = len(infiley.columns)
    colsx =  infiley.columns  #  columns (like dictionary keys)
    headlstx =  infiley.head(nocolsx)
    tailstx =  infiley.tail(nocolsx)
    dattypex =  infiley.dtypes
    print ("DataFrame columns:  ")
    print (colsx)
    print ("DataFrame head:  ")
    print (headlstx)
    print ("DataFrame tail:  ")
    print (tailstx)
    print ("Datatypes:  ")
    print (dattypex)
    naicsbse = infiley[["NAICS_CODE"]]
    print(naicsbse.head())
    print ("Type of naics code column:  ", naicsbse.dtypes)
#    print("Row 0:  ",naicsbse.loc[0])
    for iz in range(2,6):
        magval = 10**(6-iz)
        varx = "NAICS_" + str(iz)
        naicsbsex = naicsbse.rename(columns={"NAICS_CODE": varx})
        serixyz = naicsbsex.floordiv(magval, axis=1) # getnaics will be -9 for missing, appropriate code for good naics
        # now let's rename the column and make sure it is of int64 type
        dfnx.insert(iz-2,serixyz)
    dfnaics = pd.concat(dfnx, axis=1)  # concatenate the new naics codes together
#    dfnaicsx = dfnaicsxy.floordiv()
#    dfnaics = dfnaicsx.rename(columns={0: "NAICS_2", 1: "NAICS_3", 2: "NAICS_3", 3: "NAICS_2"})
 
#    dfnaics.reindex(columns=varxlst)
    nocolsx = len(dfnaics.columns)
    colsx =  dfnaics.columns  #  columns (like dictionary keys)
    headlstx =  dfnaics.head(nocolsx)
    tailstx =  dfnaics.tail(nocolsx)
    dattypex =  dfnaics.dtypes
    print ("DataFrame columns:  ")
    print (colsx)
    print ("DataFrame head:  ")
    print (headlstx)
    print ("DataFrame tail:  ")
    print (tailstx)
    print ("Datatypes:  ")
    print (dattypex)


# now join the whole series
    
    fullpubnaics = pd.concat([infiley, dfnaics], axis=1)
#    ckfile =
    nocolsx = len(fullpubnaics.columns)
    colsx =  fullpubnaics.columns  #  columns (like dictionary keys)
    headlstx =  fullpubnaics.head(nocolsx)
    tailstx =  fullpubnaics.tail(nocolsx)
    dattypex =  fullpubnaics.dtypes
    print ("DataFrame columns:  \n")
    print (colsx)
    print ("DataFrame head:  \n")
    print (headlstx)
    print ("DataFrame tail:  \n")
    print (tailstx)
    print ("Datatypes:  \n")
    print (dattypex)

# crosstabs
    # crosstabs    
#    print("Columns in fullpubnaics are: ")
#    print(fullpubnaics.columns)
  #    more crosstabs
    print("Crosstab 1: 3-digit NAICS code, Year.") 
    print(pd.crosstab(fullpubnaics["NAICS_3"],  fullpubnaics["REPORTING_YEAR"], dropna= False))
 
    print("Crosstab 2:  3-digit NAICS code, subparts reported.")
    print(pd.crosstab(fullpubnaics["NAICS_3"],  fullpubnaics["REPORTED_SUBPARTS"], dropna= False))    
 
    print("Crosstab 3:  3-digit NAICS code, Facility ID, and reporting year.")
    print(pd.crosstab(fullpubnaics["NAICS_3"], [ fullpubnaics["FACILITY_ID"],  fullpubnaics["REPORTING_YEAR"]], dropna= False))
    
    fullpubnaics.to_pickle(out_pickle)
    
    return out_pickle
#  
    #
    # NEXT loop creates NAICS codes -- 2-digit through 5 digit
    #
#
def naicsdigits(naicsx,ix):
    import math
    tenval = 10**-(6-ix)
    return min(naicsx,math.trunc(naicsx*tenval))

    
    
    
#
def data_pandasB(in_list,out_filex):  # takes list input and writes it out to a pandas DataFrame with compression
    # imports
#    import json
    import pandas as pd
    import gzip
#    import pyarrow 
    # import pyarrow.parquet
#    import msgpack 
    # import math 
#    import fastparquet 
#    import pyarrow
    # initialize
    dataz = []
    # load the json file
    # now create a pandas DataFrame 
    dataz = in_list
    zdata = pd.DataFrame(dataz)
#    ztdata = zdata.transpose(copy=True)
    # print(type(zdata))
    nocolsx = len(zdata.columns)
    colsx = zdata.columns  #  columns (like dictionary keys)
    headlstx = zdata.head(nocolsx)
    tailstx = zdata.tail(nocolsx)
    dattypex = zdata.dtypes
    print("DataFrame columns:  ", colsx, "; DataFrame head:  ", headlstx, "; DataFrame tail:  ", tailstx, "; datatypes:  ", dattypex)
    # now write data using parquet
    # zdata.to_parquet(out_filex)
    zdata.to_pickle(out_filex)
    return 

#
#def panda_mergx(<infilestring>):   # merges together several files; designed to be called in a funciton 
#
#    import pandas as cute    
#
#    filexlst = infilestring.split(",")
#    for idx in range(1:len(filexlst)):
        
        
    #    
def data_kleanB(in_file):
    # imports
    import json
    import pandas as pd
    import msgpack
    import math 
    # initialize
    dataz = []
    # load the json file
    with open (in_file,'r') as infilex:
        dataz = json.load(infilex)
        for zolk in dataz:
            zolk["NAICS_3"] = trunc(zolk["NAICS_CODE"]*10**-3)
            zolk["NAICS_4"] = trunc(zolk["NAICS_CODE"]*10**-2)
    # now create a pandas DataFrame 
    zdata = pd.DataFrame(dataz)
    # now 
    print("Crosstab 1: 4-digit NAICS code, Year.") 
    pd.crosstab(zdata["NAICS_4"], zdata["REPORTING_YEAR"])
    print("Crosstab 2:  Facility ID, Year, 3-digit NAICS code.")
    pd.crosstab(zdata["FACILITY_ID"], [zdata["NAICS_4"], zdata["REPORTING_YEAR"]])
    print("Crosstab 3:  Facility ID & subpart list.")
    # print(zcrosstab)
    # now write a msgpack file from our panda
    msgpack.packb(zdata)  # this should do it
    return 

# does an inner merge (default) on passed keylist. Returns the out datafile
def mergex(leftpanda,rightpanda,howvarx,keylst):
    import pandas as pd
    outpanda = pd.merge(leftpanda,rightpanda,how=howvarx,on=keylst)
    return outpanda
#
# does two things:  splits the subpartlist and adds gascodes    
def misc_ghg_df(subdirx,infilx,outfilx):
    # imports
    import pandas as pd
    from utility_functions import pickled_pandas_2 as write_fl # pickled_pandas(dfx,pathx,subpt,zed,endz):    #
    from data_clean_fns import mergex
#
    gascode = {}
    gasname = {}
    subptlet = {}
    subptdesc = {}
    keylstx = []
    #
    # gas code and gas name, to match up with GAS_ID in PUB_FACTS_SUBP_GHG_EMISSION_all
    #
    #
    gascode = {1:"CO2", 2:"CH4", 3:"N2O", 6:"SF6", 7:" CHF3", 8:"BIOCO2", 
               9:"NF3", 10:" HFC", 11:"PFC", 12:"HFE", 13:"Other", 
               14:"Other_L", 15:"Very_Short", 16:"Other_Full"}
    gasname = {1:"Carbon Dioxide", 2:"Methane", 3:"Nitrous Oxide", 
               6:"Sulfur Hexafluoride", 7:"Fluoroform", 8:"Biogenic CO2", 
               9:"Nitrogen Triflouride", 10:"HFCs", 11:"PFCs", 12:"HFEs", 13:"Other", 
               14:"Fluorinated GHG Production", 15:"Very Short-lived Compounds", 
               16:"Other Fully Fluorinated GHGs"}
    
    #
    #  subpart letter and subpart description, to match up with <subpart ID in>
    #
    subptlet = {1:"C",  2:"D",  3:"E",  4:"F",  5:"G",  6:"H",  9:"K",  12:"N",  13:"O",  14:"P",  
                15:"Q",  16:"R",  17:"S",  19:"V",  21:"X",  22:"Y",  23:"Z",  24:"AA",  25:"BB",  26:"CC",  
                28:"EE",  30:"GG",  31:"HH",  35:"LL",  36:"MM",  37:"NN",  38:"OO",  
                39:"PP",  41:"U",  42:"C(Abbr)",  
                43:"I",  44:"L",  45:"T",  46:"W",  47:"DD",  48:"FF",  49:"II",  50:"QQ",  
                51:"RR",  52:"SS",  53:"TT",  54:"UU"}
    #
    subptdesc = {1:"Stationary Combustion", 2:"Electricity Generation", 
                 3:"Adipic Acid Production", 4:"Aluminum Production", 
                 5:"Ammonia Manufacturing", 6:"Cement Production", 
                 9:"Ferroalloy Production", 12:"Glass Production", 
                 13:"HCFC-22 Production and HFC-23 Destruction", 
                 14:"Hydrogen Production", 15:"Iron and Steel Production", 
                 16:"Lead Production", 17:"Lime Production", 
                 19:"Nitric Acid Production", 21:"Petrochemical Production", 
                 22:"Petroleum Refining", 23:"Phosphoric Acid Production", 
                 24:"Pulp and Paper Manufacturing", 25:"Silicon Carbide Production", 
                 26:"Soda Ash Manufacturing", 28:"Titanium Dioxide Production", 
                 30:"Zinc Production", 31:"Municipal Landfills", 
                 35:"Coal-based Liquid Fuel Supply", 36:"Petroleum Product Supply", 
                 37:"Natural Gas and Natural Gas Liquid Supply", 
                 38:"Non-CO2 Industrial Gas Supply", 39:"Carbon Dioxide (CO2) Supply", 
                 41:"Miscellaneous Uses of Carbonate", 42:"Stationary Combustion", 
                 43:"Electronics Manufacture", 44:"Fluorinated GHG Production", 
                 45:"Magnesium Production", 46:"Petroleum and Natural Gas Systems", 
                 47:"SF6 from Electrical Equipment", 48:"Underground Coal Mines", 
                 49:"Industrial Wastewater Treatment", 
                 50:"Import and Exports of Equipment with Fluorinated GHGs", 
                 51:"Geologic Sequestration of Carbon Dioxide", 
                 52:"Manufacture of Electric Transmission and Distribution Equipment", 
                 53:"Industrial Waste Landfills", 54:"Injection of Carbon Dioxide"}

    # create datafromes from dict for gas names
    # gascode_pd = pd.DataFrame(data=gascode,orient="index",columns=["GAS_ID","GAS_FORMULA"])
    gascode_pd = pd.DataFrame.from_dict(gascode,orient="index",columns=["GAS_FORMULA"])
    gasname_pd = pd.DataFrame.from_dict(gasname,orient="index",columns=["GAS_NAME"])
    # full_gas_pd = mergex(gascode_pd,gasname_pd,"outer",["GAS_ID"])
    full_gas_pd = gascode_pd.join(gasname_pd) 
    #
    # create datafromes from for subparts 
    sublet_pd = pd.DataFrame.from_dict(subptlet,orient="index",columns=["SUB_PART_LETTER"])
    subdesc_pd = pd.DataFrame.from_dict(subptdesc,orient="index",columns=["SUB_PART_DESC"])
    # full_sub_pd = mergex(sublet_pd,subdesc_pd,"outer",["SUB_PART_ID"]) 
    full_sub_pd = sublet_pd.join(subdesc_pd)
    #AK
    #
    # now we load the subpart file
    #
    bigpanda_file = subdirx + infilx
    #
    bigpanda = pd.read_pickle(bigpanda_file)
    #
    # take subsets of the big panda file---we only need to join on these columns
    #
    # big_gas_sub = bigpanda[["GAS_ID","SUB_PART_ID"]]
    #
    # big_sub = bigpanda[["SUB_PART_ID"]]
    #
    # merge gasses and main subpart file
    bigpanda_gas = bigpanda.join(full_gas_pd,how="left",on="GAS_ID") # mergex(bigpanda,full_gas_pd,"inner",["GAS_ID"])
    # merge subparts into result of previous merger
    bigpanda_all = bigpanda_gas.join(full_sub_pd,how="left",on="SUB_PART_ID")
    #
    print("Big panda gas columns, head & tail.")
    print(bigpanda_gas.columns)
    print(bigpanda_gas.head(15))
    print(bigpanda_gas.tail(15))
    #
    print("Big panda all, head & tail.")
    print(bigpanda_all.columns)
    print(bigpanda_all.head(15))
    print(bigpanda_all.tail(15))

   
    # bigpanda_all = mergex(bigpanda_gas,full_sub_pd,"inner",["SUB_PART_ID"])
    #
    newpanda = bigpanda_all.rename(columns={"YEAR": "REPORTING_YEAR"})

    # we will merge in the pub_dim_facility and split the list in some other program
    
    # intpanda1 = infilex["REPORTED_SUBPARTS"].str.split(",")
    
   # pandalst = [mpandagn, intpanda1]

     # newfilex = pd.concat(filelstx,axis=0) # ,ignore_index=True)
    
    # newpanda = pd.concat(pandalst,axis=1)
    
    print("Crosstab 1A: REPORTING_YEAR, GAS_ID, GAS_NAME.") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["GAS_ID"],  newpanda["GAS_NAME"]], dropna= False))
#
    print("Crosstab 1B: REPORTING_YEAR, GAS_ID, GAS_FORMULA.") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["GAS_ID"],  newpanda["GAS_FORMULA"]], dropna= False))
#
#   now print subparts        
#
    print("Crosstab 2A: REPORTING_YEAR, SUB_PART_ID, SUB_PART_LETTER.") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["SUB_PART_ID"],  newpanda["SUB_PART_LETTER"]], dropna= False))
    
    print("Crosstab 2B: REPORTING_YEAR, SUB_PART_ID, SUB_PART_DESC.") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["SUB_PART_ID"],  newpanda["SUB_PART_DESC"]], dropna= False))
    
    subdirz = subdirx + "combined_datasets/"
    
    write_fl(newpanda,subdirz,outfilx)

    return # no arguments yet
# like previous program, but for sectors/subsectors instead of subparts
#
def misc_ghg_sector(subdirx,infilx,outfilx):
    # imports
    import pandas as pd
    from utility_functions import pickled_pandas_2 as write_fl # pickled_pandas(dfx,pathx,subpt,zed,endz):    #
    from data_clean_fns import mergex
#
    gascode = {}
    gasname = {}
    sectordict = {}
    subsectordict = {}
    typedictx = {}
    # keylstx = []
    # fullpubzx = fullpubz.astype(dtype=typedictx)
    typedictx = {"FACILITY_ID": "int64", "REPORTING_YEAR": "int64"}
    #
    # gas code and gas name, to match up with GAS_ID in PUB_FACTS_SUBP_GHG_EMISSION_all
    #
    #
    gascode = {1:"CO2", 2:"CH4", 3:"N2O", 6:"SF6", 7:" CHF3", 8:"BIOCO2", 
               9:"NF3", 10:" HFC", 11:"PFC", 12:"HFE", 13:"Other", 
               14:"Other_L", 15:"Very_Short", 16:"Other_Full"}
    gasname = {1:"Carbon Dioxide", 2:"Methane", 3:"Nitrous Oxide", 
               6:"Sulfur Hexafluoride", 7:"Fluoroform", 8:"Biogenic CO2", 
               9:"Nitrogen Triflouride", 10:"HFCs", 11:"PFCs", 12:"HFEs", 13:"Other", 
               14:"Fluorinated GHG Production", 15:"Very Short-lived Compounds", 
               16:"Other Fully Fluorinated GHGs"}
    
    #
    #  
    #  Sector ID dictionary---match these to pub_dim_facts . . .(key is sector ID;--values sector name, sector type)
    #
    sectordict = {2: ["Waste","E"], 3: ["Power Plants","E"], 4: ["Refineries","E"], 
                  5: ["Chemicals","E"], 6: ["Metals","E"], 7: ["Pulp and Paper","E"], 8: ["Minerals","E"], 
                  9: ["Coal-based Liquid Fuel Supply","S"], 10: ["Petroleum Product Suppliers","S"], 
                  11: ["Natural Gas and Natural Gas Liquids Suppliers","S"], 12: ["Industrial Gas Suppliers","S"], 
                  13: ["Suppliers of CO2","S"], 14: ["Other","E"], 15: ["Petroleum and Natural Gas Systems","E"], 
                  16: ["Import and Export of Equipment Containing Fluorintaed GHGs","S"], 17: ["Injection of CO2","I"]} 

#
    # Subsector Id dictionary –subsector id (key), subsector name, subsector desc, sector id
    subsectordict = {1: ["D","Power Plants",  3 ], 2:["E","Adipic Acid Production",  5 ], 3:["F","Aluminum Production",  6 ], 
                     4:["G","Ammonia Manufacturing",  5 ], 5:["H","Cement Production",  8 ], 6:["K","Ferroalloy Production",  6 ], 
                     7:["N","Glass Production",  8 ], 8:["O","HCFC-22 Prod./HFC-23 Dest.",  5 ], 9:["P","Hydrogen Production",  5 ], 
                     10:["Q","Iron and Steel Production",  6 ], 11:["R","Lead Production",  6 ], 12:["S","Lime Manufacturing",  8 ], 
                     13:["V","Nitric Acid Production",  5 ], 14:["X","Petrochemical Production",  5 ], 
                     15:["Y","Petroleum Refineries",  4 ], 16:["Z","Phosphoric Acid Production",  5 ], 17:["AA","Pulp and Paper",  7 ], 
                     18:["BB","Silicon Carbide Production",  5 ], 19:["CC","Soda Ash Manufacturing",  8 ], 20:["EE","Titanium Dioxide Production",  5 ], 
                     21:["GG","Zinc Production",  6 ], 22:["HH","Municipal Landfills",  2 ], 24:["C_FOOD","Food Processing",  14 ], 
                     25:["C_ETHANOL","Ethanol Production",  14 ], 26:["C_MANUF","Manufacturing",  14 ], 27:["C_OTHER","Other",  14 ], 
                     28:["C_MIL","Military",  14 ], 29:["C_UNIV","Universities",  14 ], 
                     34:["C_CHEM","Other Chemicals",  5 ], 35:["C_METAL","Other Metals",  6 ], 
                     36:["C_MINERAL","Other Minerals",  8 ], 37:["C_PAPER","Other Paper Producers",  7 ], 38:["PRO","Producer",  9 ], 
                     39:["IMP","Importer",  9 ], 40:["EXP","Exporter",  9 ], 41:["PRO","Producer",  10 ], 
                     42:["IMP","Importer",  10 ], 43:["EXP","Exporter",  10 ], 44:["PRO","Producer",  12 ], 45:["IMP","Importer",  12 ], 
                     46:["EXP","Exporter",  12 ], 48:["IMP","Importer",  13 ], 49:["EXP","Exporter",  13 ], 
                     50:["LDC","Natural Gas Distribution",  11 ], 51:["NGL","Natural Gas Liquids Fractionation",  11 ], 
                     52:["W1","Offshore Petroleum & Natural Gas Production",  15 ], 53:["W2","Onshore Petroleum & Natural Gas Production",  15 ], 
                     54:["W3","Natural Gas Processing",  15 ], 55:["W4","Natural Gas Transmission/Compression",  15 ], 
                     56:["W8","Natural Gas Local Distribution Companies",  15 ], 57:["W5","Underground Natural Gas Storage",  15 ], 
                     58:["W6","Liquefied Natural Gas Storage",  15 ], 59:["W7","Liquefied Natural Gas Imp./Exp. Equipment",  15 ], 
                     60:["L","Fluorinated GHG Production",  5 ], 61:["FF","Underground Coal Mines",  14 ], 
                     62:["DD","Use of Electrical Equipment",  14 ], 63:["I","Electronics Manufacturing",  14 ], 
                     64:["SS","Electrical Equipment Manufacturers",  14 ], 65:["T","Magnesium",  6 ], 66:["TT","Industrial Landfills",  2 ], 
                     67:["II","Wastewater Treatment",  2 ], 68:["C_COMB","Solid Waste Combustion",  2 ], 
                     69:["IMP","Importer",  16 ], 70:["EXP","Exporter",  16 ], 71:["UU","Injection of Carbon Dioxide",  17 ], 
                     72:["RR","Geologic Sequestration of Carbon Dioxide",  17 ], 73:["CAPTURE","CO2 Capture",  13 ], 
                     74:["PROD_WELLS","CO2 Production Wells",  13 ], 75:["W_Other","Other Petroleum and Natural Gas Systems",  15 ], 
                     76:["W9","Petroleum & Natural Gas Gathering & Boosting",  15 ], 77:["W10","Natural Gas Transmission Pipelines",  15 ]}

     

    # create datafromes from dict for gas names
    # gascode_pd = pd.DataFrame(data=gascode,orient="index",columns=["GAS_ID","GAS_FORMULA"])
    gascode_pd = pd.DataFrame.from_dict(gascode,orient="index",columns=["GAS_FORMULA"])
    gasname_pd = pd.DataFrame.from_dict(gasname,orient="index",columns=["GAS_NAME"])
    # full_gas_pd = mergex(gascode_pd,gasname_pd,"outer",["GAS_ID"])
    full_gas_pd = gascode_pd.join(gasname_pd) 
    #
    # create datafromes from for subparts 
    sector_pd = pd.DataFrame.from_dict(sectordict,orient="index",columns=["SECTOR_NAME","SECTOR_TYPE"])
    subsector_pdx = pd.DataFrame.from_dict(subsectordict,orient="index",columns=["SUBSECTOR_NAME","SUBSECTOR_DESC","SECTOR_ID"])
    
    # full_sub_pd = merge sector & subsector
    # sector_pd = sector_pdx.set_index("SECTOR_ID")
    # RESET THE index of the sector ID
    subsector_pd = subsector_pdx.reset_index()
    # rankings_pd.rename(columns = {'test':'TEST'}, inplace = True)
    subsector_pd.rename(columns = {"index": "SUBSECTOR_ID"}, inplace = True)

    full_sub_pd = subsector_pd.join(sector_pd,how="left",on="SECTOR_ID")  # mergex(sector_pd,subsector_pd,"outer",["SECTOR_ID"])
    #AK
    print("Subsector file:  : columns, head & tail.\n")
    print(subsector_pd.columns)
    print(subsector_pd.head(15))
    print(subsector_pd.tail(15))
    #
    #
    print("Sector & subsector file:  columns, head & tail.\n")
    print(full_sub_pd.columns)
    print(full_sub_pd.head(15))
    print(full_sub_pd.tail(15))

    #
    # now we load the subpart file
    #
    bigpanda_file = subdirx + infilx
    #
    bigpanda = pd.read_pickle(bigpanda_file)
    #
    # smallpanda = bigpanda[["GAS_ID"]]
    #
    # take subsets of the big panda file---we only need to join on these columns
    #
    # big_gas_sub = bigpanda[["GAS_ID","SUB_PART_ID"]]
    #
    # big_sub = bigpanda[["SUB_PART_ID"]]
    #
    # merge gasses and main subpart file
    bigpanda_gas = bigpanda.join(full_gas_pd,how="left",on="GAS_ID") # mergex(bigpanda,full_gas_pd,"inner",["GAS_ID"])
    # merge subparts into result of previous merger
    # mergex(leftpanda,rightpanda,howvarx,keylst):
    bigpanda_allxy = mergex(bigpanda_gas,full_sub_pd,"outer",["SECTOR_ID","SUBSECTOR_ID"])
    #
    bigpanda_all = bigpanda_allxy.fillna(-9)
    #
    #
    print("Big panda gas: columns, head & tail.\n")
    print(bigpanda_gas.columns)
    print(bigpanda_gas.head(15))
    print(bigpanda_gas.tail(15))
    #
    #
    print("Big panda all:  columns, head & tail.\n")
    print(bigpanda_all.columns)
    print(bigpanda_all.head(15))
    print(bigpanda_all.tail(15))

   
    # bigpanda_all = mergex(bigpanda_gas,full_sub_pd,"inner",["SUB_PART_ID"])
    #
    newpandax = bigpanda_all.rename(columns={"YEAR": "REPORTING_YEAR"})

    newpanda = newpandax.astype(dtype=typedictx)


    # we will merge in the pub_dim_facility and split the list in some other program
    
    # intpanda1 = infilex["REPORTED_SUBPARTS"].str.split(",")
    
   # pandalst = [mpandagn, intpanda1]

     # newfilex = pd.concat(filelstx,axis=0) # ,ignore_index=True)
    
    # newpanda = pd.concat(pandalst,axis=1)
    
    print("Crosstab 1A: REPORTING_YEAR, GAS_ID, GAS_NAME.\n") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["GAS_ID"],  newpanda["GAS_NAME"]], dropna= False))
#
    print("Crosstab 1B: REPORTING_YEAR, GAS_ID, GAS_FORMULA.\n") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["GAS_ID"],  newpanda["GAS_FORMULA"]], dropna= False))
#
#   now print subparts        
#
    print("Crosstab 2A: REPORTING_YEAR, SECTOR_ID, SUBSECTOR_ID.\n") #
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["SECTOR_ID"],  newpanda["SUBSECTOR_ID"]], dropna= False))
#
    print("Crosstab 2b: REPORTING_YEAR, SECTOR_ID, SUBSECTOR_ID.\n") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["SECTOR_ID"],  newpanda["SECTOR_NAME"]], dropna= False))
#
#
    print("Crosstab 2C: REPORTING_YEAR, SECTOR_ID, SUBSECTOR_ID.\n") 
    print(pd.crosstab(newpanda["REPORTING_YEAR"], [ newpanda["SUBSECTOR_ID"],  newpanda["SUBSECTOR_NAME"]], dropna= False))
    
    
    
    subdirz = subdirx + "combined_datasets/"
    
    write_fl(newpanda,subdirz,outfilx)

    return # no arguments yet

    
#
def misc_ghg_df_test(subdirx,infilx,outfilx):
    # imports
    import pandas as pd
    from utility_functions import pickled_pandas_2 as write_fl # pickled_pandas(dfx,pathx,subpt,zed,endz):    #
    from data_clean_fns import mergex
#
    gascode = {}
    gasname = {}
    subptlet = {}
    subptdesc = {}
    keylstx = []
    #
    # gas code and gas name, to match up with GAS_ID in PUB_FACTS_SUBP_GHG_EMISSION_all
    #
    #
    gascode = {1:"CO2", 2:"CH4", 3:"N2O", 6:"SF6", 7:" CHF3", 8:"BIOCO2", 
               9:"NF3", 10:" HFC", 11:"PFC", 12:"HFE", 13:"Other", 
               14:"Other_L", 15:"Very_Short", 16:"Other_Full"}
    gasname = {1:"Carbon Dioxide", 2:"Methane", 3:"Nitrous Oxide", 
               6:"Sulfur Hexafluoride", 7:"Fluoroform", 8:"Biogenic CO2", 
               9:"Nitrogen Triflouride", 10:"HFCs", 11:"PFCs", 12:"HFEs", 13:"Other", 
               14:"Fluorinated GHG Production", 15:"Very Short-lived Compounds", 
               16:"Other Fully Fluorinated GHGs"}
    
    #
    #  subpart letter and subpart description, to match up with <subpart ID in>
    #
    subptlet = {1:"C",  2:"D",  3:"E",  4:"F",  5:"G",  6:"H",  9:"K",  12:"N",  13:"O",  14:"P",  
                15:"Q",  16:"R",  17:"S",  19:"V",  21:"X",  22:"Y",  23:"Z",  24:"AA",  25:"BB",  26:"CC",  
                28:"EE",  30:"GG",  31:"HH",  35:"LL",  36:"MM",  37:"NN",  38:"OO",  
                39:"PP",  41:"U",  42:"C(Abbr)",  
                43:"I",  44:"L",  45:"T",  46:"W",  47:"DD",  48:"FF",  49:"II",  50:"QQ",  
                51:"RR",  52:"SS",  53:"TT",  54:"UU"}
    #
    subptdesc = {1:"Stationary Combustion", 2:"Electricity Generation", 
                 3:"Adipic Acid Production", 4:"Aluminum Production", 
                 5:"Ammonia Manufacturing", 6:"Cement Production", 
                 9:"Ferroalloy Production", 12:"Glass Production", 
                 13:"HCFC-22 Production and HFC-23 Destruction", 
                 14:"Hydrogen Production", 15:"Iron and Steel Production", 
                 16:"Lead Production", 17:"Lime Production", 
                 19:"Nitric Acid Production", 21:"Petrochemical Production", 
                 22:"Petroleum Refining", 23:"Phosphoric Acid Production", 
                 24:"Pulp and Paper Manufacturing", 25:"Silicon Carbide Production", 
                 26:"Soda Ash Manufacturing", 28:"Titanium Dioxide Production", 
                 30:"Zinc Production", 31:"Municipal Landfills", 
                 35:"Coal-based Liquid Fuel Supply", 36:"Petroleum Product Supply", 
                 37:"Natural Gas and Natural Gas Liquid Supply", 
                 38:"Non-CO2 Industrial Gas Supply", 39:"Carbon Dioxide (CO2) Supply", 
                 41:"Miscellaneous Uses of Carbonate", 42:"Stationary Combustion", 
                 43:"Electronics Manufacture", 44:"Fluorinated GHG Production", 
                 45:"Magnesium Production", 46:"Petroleum and Natural Gas Systems", 
                 47:"SF6 from Electrical Equipment", 48:"Underground Coal Mines", 
                 49:"Industrial Wastewater Treatment", 
                 50:"Import and Exports of Equipment with Fluorinated GHGs", 
                 51:"Geologic Sequestration of Carbon Dioxide", 
                 52:"Manufacture of Electric Transmission and Distribution Equipment", 
                 53:"Industrial Waste Landfills", 54:"Injection of Carbon Dioxide"}

    # create datafromes from dict for gas names
    # gascode_pd = pd.DataFrame(data=gascode,orient="index",columns=["GAS_ID","GAS_FORMULA"])
    gascode_pd = pd.DataFrame.from_dict(data=gascode,orient="index",columns=["GAS_FORMULA"])
    gasname_pd = pd.DataFrame.from_dict(gasname,orient="index",columns=["GAS_NAME"])
    # full_gas_pd = mergex(gascode_pd,gasname_pd,"outer",["GAS_ID"])
    full_gas_pd = gascode_pd.join(gasname_pd) 
    print("gas name panda")
    print(full_gas_pd)
    #
    # create datafromes from for subparts 
    sublet_pd = pd.DataFrame.from_dict(subptlet,orient="index",columns=["SUB_PART_LETTER"])
    subdesc_pd = pd.DataFrame.from_dict(subptdesc,orient="index",columns=["SUB_PART_DESC"])
    print("subdesc panda")
    print(subdesc_pd)
    # full_sub_pd = mergex(sublet_pd,subdesc_pd,"outer",["SUB_PART_ID"]) 
    full_sub_pd = sublet_pd.join(subdesc_pd)
    print("subpart panda")
    print(full_sub_pd)
    #AK
    #
    # now we load the subpart file
    #
    bigpanda_file = subdirx + infilx
    #
    bigpanda = pd.read_pickle(bigpanda_file)
    #
    # take subsets of the big panda file---we only need to join on these columns
    #
    big_gas = bigpanda[["GAS_ID"]]
    #
    big_sub = bigpanda[["SUB_PART_ID"]]
    #
    # merge gasses and main subpart file
    bigpanda_gas = big_gas.join(full_gas_pd,how="left",on="GAS_ID") # mergex(bigpanda,full_gas_pd,"inner",["GAS_ID"])
    # merge subparts into result of previous merger
    bigpanda_sub = big_sub.join(full_sub_pd,how="left",on="SUB_PART_ID")
    #
    print("Big panda gas columns, head & tail.")
    print(bigpanda_gas.columns)
    print(bigpanda_gas.head(15))
    print(bigpanda_gas.tail(15))
    #
    print("Big panda sub columns, head & tail.")
    print(bigpanda_sub.columns)
    print(bigpanda_sub.head(15))
    print(bigpanda_sub.tail(15))

    
    return # no arguments

# merges subpart or sector file with pub dim facility; derived from intake_ghgrp_PUB_ff

def merge_ghgrp_PUB_files(file_dir,filelst,outpicklst): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
#    from ghgrp_list_func import pubdimfacility   # the subpart functions
    from data_clean_fns import mergex # mergex(leftpanda,rightpanda,howvarx,keylst): 
    import pandas as pd
#    import math 
#    file_tx = []
    dflstx = []
#    serix = []
    pandaxyz = []
    pandaxy = []
    pandax = []
    dattypex = []
    colx = []
#    setlistx = []
    typedictx = {}  # apply this just before the write
    typedictx = {"FACILITY_ID": "int64",  "CITY": "str", "STATE": "str", "ZIP": "int64",   
                 "COUNTY_FIPS": "int64", "COUNTY": "str", "ADDRESS1": "str", "ADDRESS2": "str", "FACILITY_NAME": "str", "STATE_NAME": "str",
                 "NAICS_CODE": "int64", "BAMM_USED_DESC": "str", "EMISSION_CLASSIFICATION_CODE": "str", "PROGRAM_NAME": "str",
                 "CEMS_USED": "str", "CO2_CAPTURED": "str", "REPORTED_SUBPARTS": "str",
                 "BAMM_APPROVED": "str", "EMITTED_CO2_SUPPLIED": "str", 
                 "PUBLIC_HTML": "str", "PUBLIC_XML": "str", "LOCATION": "str", "PARENT_COMPANY": "str", "PUBLIC_XML_XML": "str",
                 "REPORTED_INDUSTRY_TYPES": "str", "FACILITY_TYPES": "str", "SUBMISSION_ID": "int64",  "UU_RD_EXEMPT": "str", "REPORTING_STATUS": "str",
                 "PROCESS_STATIONARY_CML": "str", "COMMENTS": "str", "RR_MRV_PLAN_URL": "str", "RR_MONITORING_PLAN": "str", "RR_MONITORING_PLAN_FILENAME": "str",
                 "GAS_ID": "int64"}
    # need to build a loop here with the appropriate "pub" file. 
    for ix in range(len(filelst)):  # filelst input is range
        # file input and output names
        # end file input and outpu"t names
        if ix > 0:
            file_dirx = file_dir + "combined_datasets/"
        else:
            file_dirx = file_dir
        # replace this with unpicklinga
        filex = file_dirx + filelst[ix]
        print("Name of input file:  ",filex)
        dflstx.insert(ix,pd.read_pickle(filex))
        # print("last element of dlfstx is ", dflstx[-1])
    # now we will merge--- this is a custom coding; the pub_dim facility will be first on list [element 0], 
    # and separately merged with subpart and sector datasets --- elements 1 and 2
    # print ranges for testing
    for iy in range(len(dflstx)):
        print(str(iy) + " element in data list is \n",dflstx[iy].head())
#    print("lenght of datalist is", str(len(dflstx)), "\n length of output list is ", str(len(outpicklst)))
    for ix in range(1,len(dflstx)):  # remember to define all pandas datalists above
        print("ix loop number is", str(ix))
        pandaxyz = mergex(dflstx[0],dflstx[ix],"inner",["FACILITY_ID","REPORTING_YEAR"]) # 
        pandaxy = pandaxyz.assign(reported_subpart_list = pandaxyz["REPORTED_SUBPARTS"].str.split(","))
        pandax = pandaxy.astype(dtype=typedictx)
        dattypex = pandax.dtypes
        print("Datatypes of merged file ", pandax, ": \n", dattypex)
    # now let's select by columns so we can do cross tabs and see them
        colx = pandax.columns
        incx = 5
        for iz in range(0,len(colx),incx):
            if iz == 0:
                dframex = pandax.iloc[:,iz:iz+incx]
            else:
                if iz+incx <= len(colx)-1:
                    dframex = pandax.iloc[:,iz+1:iz+incx]
                else:
                    dframex = pandax.iloc[:,iz+1:-1]
                    print("Head of 5-column datasets.\n")                    
            print(dframex.head(29))
            print("Columns of 5-column datasets.\n")
            print(dframex.columns)
#         print(pd.crosstab(fullpub[ix-1], fullpub["REPORTING_YEAR"], dropna= False))
# crosstabs take 5 columns at a time, so we can see everything
#            print("Cross tabs for all elements in dataset ", pandax, "by reporting year:  \n")                    
#            print(pd.crosstab(panda"fx["REPORTING_YEAR"], , dropna=False))
        # now let's write the files
        filedirz = file_dir + "combined_datasets"
        out_pickle = filedirz + outpicklst[ix-1]
        pandax.to_pickle(out_pickle)
    return # no arguments 
# like previous function, but for merging subparts
def merge_ghgrp_SUBP_files(file_dir,filelst,outpicklst): # (file_out): # inputs,  file, -- takes the PUB file & shows if there are problems with it
#    from ghgrp_list_func import pubdimfacility   # the subpart functions
    from data_clean_fns import mergex # mergex(leftpanda,rightpanda,howvarx,keylst): 
    import pandas as pd
#    import math 
#    file_tx = []
    dflstx = []
    pandax = []
    dattypex = []
    colx = []
#    setlistx = []
    # need to build a loop here with the appropriate "pub" file. 
    for ix in range(len(filelst)):  # filelst input is range
        # replace this with unpicklinga
        filex = file_dir + filelst[ix]
        print("Name of input file:  ",filex)
        dflstx.insert(ix,pd.read_pickle(filex))
        print("Number of rows & columns of " + filex, "\n", dflstx[ix].shape) 
        # print("last element of dlfstx is ", dflstx[-1])
    # now we will merge--- this is a custom coding; the pub_dim facility will be first on list [element 0], 
    # and separately merged with subpart and sector datasets --- elements 1 and 2
    # print ranges for testing
    for iy in range(len(dflstx)):
        print(str(iy) + " element in data list is \n",dflstx[iy].head())
#    print("lenght of datalist is", str(len(dflstx)), "\n length of output list is ", str(len(outpicklst)))
    for ix in range(1,len(dflstx)):  # remember to define all pandas datalists above
        print("ix loop number is", str(ix))
        pandax = mergex(dflstx[0],dflstx[ix],"inner",["FACILITY_ID","REPORTING_YEAR"]) # 
        dattypex = pandax.dtypes
        print("Datatypes of merged file ", pandax, ": \n", dattypex)
        print("Number of rows & columns of ", pandax, "\n", pandax.shape) 
    # now let's select by columns so we can do cross tabs and see them
        colx = pandax.columns
        incx = 5
        for iz in range(0,len(colx),incx):
            if iz == 0:
                dframex = pandax.iloc[:,iz:iz+incx]
            else:
                if iz+incx <= len(colx)-1:
                    dframex = pandax.iloc[:,iz+1:iz+incx]
                else:
                    dframex = pandax.iloc[:,iz+1:-1]
                    print("Head of 5-column datasets.\n")                    
            print(dframex.head(29))
            print("Columns of 5-column datasets.\n")
            print(dframex.columns)
#         print(pd.crosstab(fullpub[ix-1], fullpub["REPORTING_YEAR"], dropna= False))
# crosstabs take 5 columns at a time, so we can see everything
#            print("Cross tabs for all elements in dataset ", pandax, "by reporting year:  \n")                    
#            print(pd.crosstab(panda"fx["REPORTING_YEAR"], , dropna=False))
        # now let's write the files
#        filedirz = file_dir + "combined_datasets"
        out_pickle = file_dir + outpicklst[ix-1]
        pandax.to_pickle(out_pickle)
    return # no arguments 
#
# the following function drops variables and returns a dataframe --- to be used in another program
#    
def drop_vars(old_panda,colistx):  # this returns a dataframe, but can also write to a file
    # imports
    import pandas as pd
    #`
    Finished = False
    retlst = []
    try:
        print(colistx)
        new_panda = old_panda.drop(columns=colistx) # (axis=1,labels=colistx)
        print(new_panda.dtypes)
#
        # ["index","SUB_PART_ID","GAS_ID","GAS_FORMULA","GAS_NAME"]
#        out_pickle = file_dir + outfilex
#        new_panda.to_pickle(out_pickle)

        Finished = True
        retlst = [Finished, new_panda]        
    except KeyError as keyz:
        raise KeyError("KeyError") from keyz
        retlst = [Finished, 0]
    except Exception:
        retlst = [Finished, 0]
    return retlst
#
#    this drops variables with dataframes
#
    
#   the following function groups variables in a dataframe
#   useful for preparing a file for subpart merging 
#    
def groupies(file_dir,infilex,droplst,colistx,outfilex):    
    import pandas as pd
    from full_dataframe_analysis_fncs import prelim_pivot as pivoty
    from data_clean_fns import drop_vars as dropit # drop_vars(file_dir,infilex,colistx): 
    
    # display options
    pd.set_option('display.max_rows', 999)
    pd.set_option('precision', 2)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 120)
#
    runritelst = []
    #
    # Finished = False
    #
    try:
        in_filex = file_dir + infilex
        older_panda = pd.read_pickle(in_filex)
        runritelst = dropit(older_panda,droplst)
        if runritelst[0] == True:
            old_panda = runritelst[1]
            new_panda = old_panda.groupby(by=colistx, as_index=False).sum()
            # ["index","SUB_PART_ID","GAS_ID","GAS_FORMULA","GAS_NAME"]
            out_pickle = file_dir + outfilex
            new_panda.to_pickle(out_pickle)
            print("Variables successfully grouped. New file is " + out_pickle)
            print("New panda datatypes \n", new_panda.dtypes)
            print("First rows of dataframe \n",new_panda.head(100))
            print("Columns of new file ",new_panda.columns)
            print("Number of rows & columns of old dataframe:  ", runritelst[1].shape, "\n") 
            print("Number of rows & columns of new dataframe:  ", new_panda.shape, "\n") 
        else:
            print("Something went wrong. Variables not dropped.")
    except:
        pass
    return # no variables to return

def getdataframe(indirx,infilex):  #input is file; output is dataframe
    # imports
    import pandas as pd
    #
    filex = indirx+infilex
    outframe = pd.read_pickle(filex)
    return outframe
#
