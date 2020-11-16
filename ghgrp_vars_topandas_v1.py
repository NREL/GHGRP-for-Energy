# universal initial api call function for GHGRP --doesn't work yet like ghgrp_vars_obs_mrgx9c.py, but meant to write as a panda
def grab_ghg_url_2pandas(subx):
    import json
    import pandas as pd
    # from ghgrp_filenames import ghgrp_combineyrs as smash
    from get_url_fn import get_url_panda as goget_api
    from utility_functions import get_ghgrp_obs as get_count
    from utility_functions import pickled_pandas as write_fl # pickled_pandas(dfx,pathx,subpt,zed,endz):    #
    zpath = "L:/mid/kpe/data/api_scraping/ghgrp_write_files"  # path for writing panda
    #    
    ghgrp_init = "https://enviro.epa.gov/enviro/efservice"
    # universal penultimate end of call
    # ghgrp_penu = "/REPORTING_YEAR/=/" + str(yearx) defined in loop
    # for count variable, add to the end of universal end of call
    ghgrp_ct = "/COUNT"
    # now make the call for rows--after 
    ghgrp_rows ="/rows/"
    ghgrp_json = "/JSON"
    ghgrp_yrx = []
    ghgrp_yrz = []
    ghgrp_yrz = [[range(2011,2015)], [range(2015,2019)]] # old & new variables
    ghgrp_yrx = ["/REPORTING_YEAR/</2015", "/REPORTING_YEAR/>=/2015"] 
    yrz_indic = "/REPORTING_YEAR/=/"    # use for subpart W
    # initialize list
    SubpartXlst = []
    SubW_lst = []
    ghgrp_thew = []
    # define the a_list & the_list variables, then assign a token value 
    # so it will enter the while loop
    a_list = []
    the_list = []
    #
    # a_list = ["KP"]
    # the_list = ["is cool"]
    ##
    mult_list = [] # output of goget api function
    #
    # now assign row intervals and timeouts
    intrvl = 1500 # number of rows for subparts except Subpart W
    timex = 1200   # timeout variable    
    max_obs = 20*intrvl # at how many obs do you write a file
    #
    # lists
    # subparts
    # F = AL, G=NH4, H=Cement, K = ferroalloy N=glass
    # P = H production, Q = Iron & steel production, S = Lime manufacturing
    # W = petroleum & natural gas systems, X = petrochemicals,
    # AA = pulp & paper, NN = natural gas and natural gas liquids suppliers
    # UU = injection of carbon dioxide
    # PUB = pub dim facilit
    # subpart C
    ghgrp_spca = "/C_CEMS_QUARTERLY_CO2"
    ghgrp_spcb = "/C_CONFIGURATION_LEVEL_INFO"
    ghgrp_spcc = "/C_FUEL_LEVEL_INFORMATION"
    ghgrp_spcd = "/C_TIER1EQUATIONINPUT"
    ghgrp_spce = "/C_TIER2_MONTHLY_HHV"
    ghgrp_spcf = "/C_BIOGENIC_CO2_DETAILS"
    # subparts F-K
    ghgrp_spfa = "/F_SUBPART_LEVEL_INFORMATION"
    ghgrp_spga = "/G_SUBPART_LEVEL_INFORMATION"
    ghgrp_spgb = "/G_CEMS_DETAILS"
    ghgrp_spgc = "/G_NON_CEMS_SOURCE_INFO"
 #   ghgrp_spha = "/H_SUBPART_LEVEL_INFORMATION"
    ghgrp_spha = "/H_CEMS_DETAILS"
#
#    ghgrp_spka = "/K_SUBPART_LEVEL_INFORMATION"
    ghgrp_spka = "/K_CEMS_DETAILS"
    ghgrp_spkb = "/K_NON_CEMS_SOURCE_INFO"
    # subparts N-P
 #   ghgrp_spna = "/N_SUBPART_LEVEL_INFORMATION"
    ghgrp_spna = "/N_NON_CEMS_SOURCE_INFO"
    ghgrp_spnb = "/N_CEMS_DETAILS"
    ghgrp_sppa = "/P_SUBPART_LEVEL_INFORMATION"
    ghgrp_sppb = "/P_CEMS_DETAILS"
    # subpart Q
 #   ghgrp_spqa = "/Q_SUBPART_LEVEL_INFORMATION"
    ghgrp_spqa = "/Q_CEMS_DETAILS"
    ghgrp_spqb = "/Q_UNIT_DETAILS"
    ghgrp_spqc = "/Q_FLARE_INFORMATION"
    # subpart S
 #   ghgrp_spsa = "/S_SUBPART_LEVEL_INFORMATION"
    ghgrp_spsa = "/S_CEMS_DETAILS"
    ghgrp_spsb = "/S_NO_CEMS_ANNUAL_AVG"
    ghgrp_spsc = "/S_LIME_PRODUCT_DETAILS"
    ghgrp_spsd = "/S_LIME_BYPRODUCT_DETAILS"
    #
    #    #
    # subpart W: different names for pre-2015 and 2015+ variables
    # pre-2015
    # ghgrp_spwa = "/W_SUBPART_LEVEL_INFORMATION"
#
    ghgrp_spwa = "/W_LOCAL_DIST_COMPANIES_DETAILS"
    ghgrp_spwb = "/W_COMBUSTION"
    ghgrp_spwc = "/W_ASSO_VENTING_FLARING"
    ghgrp_spwd = "/W_ASSO_GAS_VENT_FLARE_BASIN"
    ghgrp_spwe = "/W_WELL_COMPLETION_HYDRAULIC"
    # 
    # post 2015 (different variables)
    ghgrp_spwa15 = "/EF_W_FACILITY_OVERVIEW"
    ghgrp_spwb15 = "/EF_W_EMISSIONS_SOURCE_GHG"
    ghgrp_spwc15 = "/EF_W_COMBUST_EQUIP_SUMM" #analog to pre15
    ghgrp_spwd15 = "/EF_W_ASSOCIATED_NG_SUMM"
    ghgrp_spwe15 = "/EF_W_FLARE_STACKS_SUMM" #analog to ventin
    # Subpart X
    # ghgrp_spxa = "/X_SUBPART_LEVEL_INFORMATION"
    ghgrp_spxa = "/X_CEMS_DETAILS"
    ghgrp_spxb = "/X_PROCESS_UNIT_DETAILS"
    ghgrp_spxc = "/X_COMBUSTION_ETHYLENE_UNIT"
    ghgrp_spxd = "/X_MASS_BALANCE_UNIT_DETAILS"

    # subparts AA
    # ghgrp_spaaa = "/AA_SUBPART_LEVEL_INFORMATION"
    ghgrp_spaaa = "/AA_CEMS_DETAILS"
    ghgrp_spaab = "/AA_SPENT_LIQUOR_INFORMATION"
    ghgrp_spaac = "/AA_FOSSIL_FUEL_INFORMATION"
    # subpart NN
    # ghgrp_spnna = "/NN_SUBPART_LEVEL_INFORMATION"
    ghgrp_spnna = "/NN_LDC_NAT_GAS_DELIVERIES"
    ghgrp_spnnb = "/NN_LDC_DETAILS"
    ghgrp_spnnc = "/NN_NGL_FRACTIONATOR_METHODS"
    # subpart UU
    ghgrp_spuua ="/UU_INJ_CO2_UNIT"
    ghgrp_spuub ="/UU_INJ_CO2_FACILITY"
    # Subpart PUB
    ghgrp_puba = "/PUB_DIM_FACILITY"
    ghgrp_pubb = "/PUB_FACTS_SECTOR_GHG_EMISSION"
    ghgrp_pubc = "/PUB_DIM_GHG"
    ghgrp_pubd = "/PUB_FACTS_SUBP_GHG_EMISSION"
    #
    #
    if subx == "PUB":
        SubpartXlst = [ghgrp_pubd] # Ghgrp_pubb] # , ghgrp_pubc] 
    else:
        SubpartXlst = []
        print("No such subpart in funtcion.")
        # break
#    NAICS_LST = []
#    for zerko in naics_listxx:
#        NAICS_LST.extend(zerko)
    # NAICS_LST.extend(NAICS_LST2)
    # NAICS_CT = 0   # NAICS COUNTING VARIABLE
    try:
        if subx != "W":     
            for z in SubpartXlst:
      #          max_obs_ct = 1 # multiple of number of obs
                ghgrp_url_ct = ghgrp_init + z + ghgrp_ct
                nobs = get_count(ghgrp_url_ct)
                Finished = False                
                ix = 80  # this is usually 0; there was a crash in the middle of a run around here.
          # start the ix loop
                while not Finished:
                    if ix == 0:
                        begx = 0
                        endx = intrvl
                    else:
                        begx = ix*intrvl+1
                        endx = (ix+1)*intrvl
                    if begx > nobs:   # if the the beginning read observation is greater than number of observations, we are finished before we read
                        Finished = True
                    # constructing the url to be delivered to the api 
                    else:
                        ghgrp_url = ghgrp_init + z + ghgrp_rows + str(begx) + ":" + str(endx) + ghgrp_json
                        print("Reading url ", ghgrp_url )
                     # now let's get our data       
                        mult_list = goget_api(ghgrp_url,timex) 
                     # def pickled_pandas(dfx,pathx,subpt,zed,endz):   
                        # if statement --- is calling the function that has two outputs:
                        # data from calling the api, status code = True if the api call was successful
                        if mult_list[1]: # Will be True if it ran OK 
                            # pickled_pandas(dfx,pathx,subpt,zed,endz)
                            z_pd_file = mult_list[0]
                            #
                            #
                            if ix <= 1:
                            # if it's the first round, print salient information 
                                nocolsx = len(z_pd_file.columns)
                                colsx =  z_pd_file.columns  #  columns (like dictionary keys)
                                headlstx =  z_pd_file.head(nocolsx)
                                tailstx =  z_pd_file.tail(nocolsx)
                                dattypex =  z_pd_file.dtypes
                                print ("Dataframe columns:  ")
                                print (colsx)
                                print ("Dataframe head:  ")
                                print (headlstx)
                                print ("Dataframe tail:  ")
                                print (tailstx)
                                print ("Datatypes:  ")
                                print (dattypex)
                            # end of print subroutines
                            write_fl(z_pd_file,zpath,subx,z,endx)
                            print("End of loop #" + str(ix))
                            ix += 1 # increment ix by 1
#                            if ix >= 3:   # this if statement if for testing--allows two writes 
#                                Finished = True   
                            if ix >= 3000: # this will prevent inifinite loops
                                print("Triggered loop emergency exit.")
                                break
                        else:  # if we get a bad status code
                            break  # there was a problem with the URL, so get out of this loop
                                   # do not increment, do not pass go, do not collect $200
        else: # if subx == "W":  # subpart X list for W has] two lists instead of variables, so we index z
            varx = []
            for idx in range(len(SubW_lst)): # governs the -2014 and 2015 plus
                # the varx variable, part of the write file name, can be assigned in the outer loop
                varx = ["YEARS_2011_2014", "YEARS_2015_X"]
         #
                for zippo in SubW_lst[idx]:        # now clear our lists to be consistent with list clearing in the "if" part
                    ghgrp_url_ct = ghgrp_init + zippo + ghgrp_ct
                    nobs = get_count(ghgrp_url_ct)
                    # when we write the file, filestrng will be used    
                    filestrg = zippo + varx[idx]
                    # 
                    Finished = False                
                    ix = 0
                    # start the ix loop
                    while not Finished:
                        if ix == 0:
                            begx = 0
                            endx = intrvl
                        else:
                            begx = ix*intrvl+1
                            endx = (ix+1)*intrvl
                            # check to see if our beginning observation exceeds # obs. If so, we're done.                            
                        if begx > nobs:
                            Finished = True
                        else:
                            # constructing the url to be delivered to the api 
                            ghgrp_url = ghgrp_init + zippo + ghgrp_rows + str(begx) + ":" + str(endx) + ghgrp_json
                            print("Reading url ", ghgrp_url )
                         # now let's get our data       
                            mult_list = goget_api(ghgrp_url,timex) # has list return; 0 is list, 1 is status
#
                            if mult_list[1]:
                                z_pd_file = mult_list[0]
                                write_fl(z_pd_file,zpath,subx,filestrg,endx)
                                print("End of loop #" + str(ix))
                                ix += 1 # increment ix by 1
#                                if ix >= 3:  # this if statement for testing only
#                                    Finished = True
                                if ix >= 3000: # this will prevent inifinite loops
                                    print("Triggered loop emergency exit.")
                                    break
                            else:  # if we get a bad status code
                                break  # there was a problem with the URL, so get out of this loop
                                       # do not increment, do not pass go, do not collect $200
    except TypeError as rundriper:
        raise TypeError from rundriper 
    except Exception as errx:
        print("Error {er}".format(er=errx))
        
    return # no variables to return
#
# the following function joins a bunch of pandas together and adds crosstabs.
# input:  startno, endno, intervalno, & filehead & extension    
#    
def join_pandas(dirinx, fileheadx, extd, starnox, endnox, intrvlnox, diroutx, outfilex):
    import pandas as pd
    from utility_functions import pickled_pandas_2 as write_fl # pickled_pandas(dfx,pathx,subpt,zed,endz):    #
    #
    filelstx = []
    #
    testcountx = 1
    #
    endit = endnox + intrvlnox
    #
    # print(dirinx, fileheadx, extd, starnox, endnox, intrvlnox, diroutx, outfilex, endit) #
    #
    for ix in range(starnox, endit, intrvlnox):
        filex = dirinx + fileheadx + str(ix) + extd
        
        print("Now reaading:  ", filex)
        
        filelstx.insert(-1,pd.read_pickle(filex))
        #
        # for testing nonly
        #if testcountx >=3:
        #    continue
        # testcountx += 1
    # now put together#
    newfilex = pd.concat(filelstx,axis=0) # ,ignore_index=True)
    #
    write_fl(newfilex,diroutx,outfilex)
    
    return # no return variable
    
            



    
        
    