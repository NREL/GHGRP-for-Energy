# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 16:30:27 2020

@author: KPE
"""
# this is where all the subpart drudgery goes
#
def pubdimfacility(countx):
    if countx == 0:
        return "DIM_FACILITY_30000.json"
    if countx == 1:
        return "DIM_FACILITY_60000.json"
    if countx == 2:
        return "DIM_FACILITY_82500.json"
    #
def pubfacts(countx):
    if countx == 0:    
        return "FACTS_SECTOR_GHG_EMISSION_30000.json"
    elif countx == 1:    
        return "FACTS_SECTOR_GHG_EMISSION_60000.json"
    elif countx == 2:    
        return "FACTS_SECTOR_GHG_EMISSION_90000.json"
    elif countx == 3:    
        return "FACTS_SECTOR_GHG_EMISSION_120000.json"
    elif countx == 4:    
        return "FACTS_SECTOR_GHG_EMISSION_150000.json"
    elif countx == 5:    
        return "FACTS_SECTOR_GHG_EMISSION_180000.json"
    elif countx == 6:    
        return "FACTS_SECTOR_GHG_EMISSION_210000.json"
    elif countx == 7:    
        return "FACTS_SECTOR_GHG_EMISSION_217500.json"
    else:
        print("No such file segment.")
#
#PUB_DIM_GH.json"
#PUB_FACTS_S_30000.json"
#PUB_FACTS_S_60000.json"
def SubpartAA(countx):
    if countx == 0:
        return "Subpart_AA_FOSSIL_FUEL_INFORMATION_9.json"
    elif countx == 1:
        return "Subpart_AA_SPENT_LIQUOR_INFORMATION.json"
    else:
        print("No such file segment.")
#
def SubpartC(countx):
# countx 0-9 for annual, 10-35 monthly, 36 quarterly
    if countx == 0:
        return "Subpart_C_BIOGENIC_CO2_DETAILS_9.json"
# Subpart_C_configuration level.json"
    elif countx == 1:
        return "Subpart_C_CONFIGURATION_LEVEL_INFO.json"
    # configuration fuel level
    elif countx == 2:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_30000.json"
    elif countx == 3:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_60000.json"
    elif countx == 4:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_90000.json"
    elif countx == 5:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_120000.json"
    elif countx == 6:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_150000.json"
    elif countx == 7:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_180000.json"
    elif countx == 8:
        return "Subpart_C_FUEL_LEVEL_INFORMATION_184500.json"
# tier one equation input
    elif countx == 9:
        return "Subpart_C_TIER1EQUATIONINPUT_9.json"
    # now for tier 2 monthlies -- use only if monthly output needed
    elif countx == 10:
        return "Subpart_C_TIER2_MONTHLY_HHV_30000.json"
    elif countx == 11:
        return "Subpart_C_TIER2_MONTHLY_HHV_60000.json"
    elif countx == 12:
        return "Subpart_C_TIER2_MONTHLY_HHV_90000.json"
    elif countx == 13:
        return "Subpart_C_TIER2_MONTHLY_HHV_120000.json"
    elif countx == 14:
        return "Subpart_C_TIER2_MONTHLY_HHV_150000.json"
    elif countx == 15:
        return "Subpart_C_TIER2_MONTHLY_HHV_180000.json"
    elif countx == 16:
        return "Subpart_C_TIER2_MONTHLY_HHV_210000.json"
    elif countx == 17:
        return "Subpart_C_TIER2_MONTHLY_HHV_240000.json"
    elif countx == 18:
        return "Subpart_C_TIER2_MONTHLY_HHV_270000.json"
    elif countx == 19:
        return "Subpart_C_TIER2_MONTHLY_HHV_300000.json"
    elif countx == 20:
        return "Subpart_C_TIER2_MONTHLY_HHV_330000.json"
    elif countx == 21:
        return "Subpart_C_TIER2_MONTHLY_HHV_360000.json"
    elif countx == 22:
        return "Subpart_C_TIER2_MONTHLY_HHV_390000.json"
    elif countx == 23:
        return "Subpart_C_TIER2_MONTHLY_HHV_420000.json"
    elif countx == 24:
        return "Subpart_C_TIER2_MONTHLY_HHV_450000.json"
    elif countx == 25:
        return "Subpart_C_TIER2_MONTHLY_HHV_480000.json"
    elif countx == 26:
        return "Subpart_C_TIER2_MONTHLY_HHV_510000.json"
    elif countx == 27:
        return "Subpart_C_TIER2_MONTHLY_HHV_540000.json"
    elif countx == 28:
        return "Subpart_C_TIER2_MONTHLY_HHV_570000.json"
    elif countx == 29:
        return "Subpart_C_TIER2_MONTHLY_HHV_600000.json"
    elif countx == 30:
        return "Subpart_C_TIER2_MONTHLY_HHV_630000.json"
    elif countx == 31:
        return "Subpart_C_TIER2_MONTHLY_HHV_660000.json"
    elif countx == 32:
        return "Subpart_C_TIER2_MONTHLY_HHV_690000.json"
    elif countx == 33:
        return "Subpart_C_TIER2_MONTHLY_HHV_720000.json"
    elif countx == 34:
        return "Subpart_C_TIER2_MONTHLY_HHV_750000.json"
    elif countx == 35:
        return "Subpart_C_TIER2_MONTHLY_HHV_780000.json"
# these are quarterlies -- which we will ordinarily not want
    elif countx == 36:
        return "Subpart_C_CEMS_QUARTERLY_CO2_9.json"
    else:
     print("No such segment.")   
#    
#     
def subpartF(countx):
    return "Subpart_F_SUBPART_LEVEL_INFORMATION.json"
def subpartG(countx):
    if countx == 0:
        return "Subpart_G_CEMS_DETAILS_9.json"
    elif countx == 1:
        return "Subpart_G_NON_CEMS_SOURCE_INFO_9.json"
    elif countx == 2:
        return "Subpart_G_SUBPART_LEVEL_INFORMATION_9.json"
def subpartH(countx):
    return "Subpart_H_CEMS_DETAILS_9.json"
def subpartK(countx):
    if countx == 0:
        return "Subpart_K_CEMS_DETAILS_9.json"
    elif countx == 1:
        return "Subpart_K_NON_CEMS_SOURCE_INFO_9.json"
    else:
        ("No such segment.")
def subpartN(countx):
    if countx == 0:
        return "Subpart_N_CEMS_DETAILS_9.json"
    elif countx == 1:
        return "Subpart_N_NON_CEMS_SOURCE_INFO_9.json"
    elif countx == 2:
        return "Subpart_NN_LDC_DETAILS_9.json"
    elif countx == 3:
        return "Subpart_NN_LDC_NAT_GAS_DELIVERIES_9.json"
    elif countx == 4:
        return "Subpart_NN_NGL_FRACTIONATOR_METHODS_9.json"
    else:
        print("No such segment.")
def subpartQ(countx):
    if countx == 0:
        return "Subpart_Q_CEMS_DETAILS_9.json"
    elif countx == 1:
        return "Subpart_Q_FLARE_INFORMATION_9.json"
    elif countx == 2:
        return "Subpart_Q_UNIT_DETAILS_9.json"
    else:
        print("No such segment.")
def subpartS(countx):
    if countx == 0:
        return "Subpart_S_CEMS_DETAILS_9.json"
    elif countx == 1:
        return "Subpart_S_LIME_BYPRODUCT_DETAILS_9.json"
    elif countx == 2:
        return "Subpart_S_LIME_PRODUCT_DETAILS_9.json"
    elif countx == 3:
        return "Subpart_S_NO_CEMS_ANNUAL_AVG_9.json"
    else:
        print("No such segment.")
def subpartUU(countx):
    if countx == 0:
        return "Subpart_UU_INJ_CO2_FACILITY.json"
    if countx == 1:
        return "Subpart_UU_INJ_CO2_UNIT.json"
    else:
        print("No such segment.")
# call these for subpart W, 2011-2014
def subpartW_11(countx):
    if countx == 0:
        return "Subpart_W_ASSO_GAS_VENT_FLARE_BASINYEARS_2011_2014_9.json"
    elif countx == 1:
        return "Subpart_W_ASSO_VENTING_FLARINGYEARS_2011_2014_9.json"
    elif countx == 2:
        return "Subpart_W_COMBUSTIONYEARS_2011_2014_9.json"
    elif countx == 3:
        return "Subpart_W_efserviceYEARS_2011_2014.json"
    elif countx == 4:
        return "Subpart_W_LOCAL_DIST_COMPANIES_DETAILSYEARS_2011_2014_9.json"
    elif countx == 5:
        return "Subpart_W_WELL_COMPLETION_HYDRAULICYEARS_2011_2014_9.json"
    else:
        print("no such segment.")
# call these for subpart W, years 2015+
def subpartW_15(countx):
    if countx == 0:
        return "Subpart_EF_W_ASSOCIATED_NG_SUMMYEARS_2015_X_9.json"
    elif countx == 1:
        # now emissions source
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_30000.json"
    elif countx == 2:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_60000.json"
    elif countx == 3:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_90000.json"
    elif countx == 4:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_120000.json"
    elif countx == 5:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_150000.json"
    elif countx == 6:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_180000.json"
    elif countx == 7:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_210000.json"
    elif countx == 8:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_240000.json"
    elif countx ==  9:
        return "Subpart_EF_W_EMISSIONS_SOURCE_GHGYEARS_2015_X_246000.json"
    elif countx == 10:
        # combustion equipment summary
        return "Subpart_EF_W_COMBUST_EQUIP_SUMMYEARS_2015_X_9.json"
    elif countx == 11:
# now facility overview
        return "Subpart_EF_W_FACILITY_OVERVIEWYEARS_2015_X_9.json"
    elif countx == 12:
        return "Subpart_EF_W_FLARE_STACKS_SUMMYEARS_2015_X_9.json"
    else:
        print("No such segment.")#
# subpart X
def subpartX(countx):  # note:  countx 1-4 is unit details
    if countx == 0:
        return "Subpart_X_CEMS_DETAILS.json"
    elif countx == 1:
        return "Subpart_X_COMBUSTION_ETHYLENE_UNIT.json"
    elif countx == 2:
        return "Subpart_X_MASS_BALANCE_UNIT_DETAILS_30000.json"
    elif countx == 3:
        return "Subpart_X_MASS_BALANCE_UNIT_DETAILS_46500.json"
    elif countx == 4:   #
        return "Subpart_X_PROCESS_UNIT_DETAILS.json"
    else:
        print("No such segment.")
#
