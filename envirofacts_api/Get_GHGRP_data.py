# -*- coding: utf-8 -*-
"""
Last updated 8/12/2020 by @calmc, colin.mcmillan@nrel.gov
"""
#
import pandas as pd
import requests
import xml.etree.ElementTree as et
import sys
import re
import os


class GHGRP_API():
    """
    Access GHGRP data through Envirofacts API. Currently written to
    apply only to tables C_FUEL_LEVEL_INFORMATION,
    D_FUEL_LEVEL_INFORMATION, c_configuration_level_info, and
    V_GHG_EMITTER_FACILITIES.

    Example
    -------
    Here's an example application for downloading emitter facility information
    from table V_GHG_EMITTER_FACILITIES for the 2018 reporting year::

        from Get_GHGRP_data import GHGRP_API
        emitter_facility_info = GHGRP_API().get_data(2018,
                                                     'V_GHG_EMITTER_FACILITIES')

    """

    @staticmethod
    def xml_to_df(xml_root, table_name, df_columns):
        """
        Converts elements of xml string obtained from EPA Envirofacts
        to a DataFrame.

        Parameters
        ----------
        xml_root : xml.etree.ElementTree.Element
            XML content from API call.

        table_name : string
            Name of GHGRP Envirofacts table.

        df_columns : int

        Returns
        -------
        rpd : pandas DataFrame
            Dataframe of entries from API call.
        """
        rpd = pd.DataFrame()

        for c in df_columns:
            cl = []

            for field in xml_root.findall(table_name):
                cl.append(field.find(c).text)

            cs = pd.Series(cl, name=c)
            rpd = pd.concat([rpd, cs], axis=1)

        return rpd

    @classmethod
    def get_data(cls, reporting_year, table, rows=None):
        """
        Return GHGRP data using EPA RESTful API based on specified reporting
        year and table. Tables of interest are C_FUEL_LEVEL_INFORMATION,
        D_FUEL_LEVEL_INFORMATION, c_configuration_level_info, and
        V_GHG_EMITTER_FACILITIES.
        Optional argument to specify number of table rows.

        Parameters
        ----------
        reporting_year : int
            Year of data to download.

        table : str
            Name of GHGRP Envirofacts table to acces.

        rows : int, optional
            Number of rows of data to return. Default value is None, which
            requests the API for all rows of the table.

        Returns
        -------
        ghgrp_data : pandas DataFrame
            Dataframe of data requested from the Envirofacts API for the
            defined year and table.
        """

        # Envirofacts API
        base_url = 'https://enviro.epa.gov/enviro/efservice/'

        # Different GHGRP Envirofacts tables have different naming
        # conventions for the year of data.
        if re.match(r'(V_GHG_EMITTER_)|(W_LIQUIDS_)', table):
            table_url = os.path.join(base_url+table+'/YEAR/',
                                     str(reporting_year))

        else:
            table_url = os.path.join(base_url+table+'/REPORTING_YEAR/',
                                     str(reporting_year))

        r_columns = requests.get(table_url + '/rows/0:1')
        r_columns_root = et.fromstring(r_columns.content)

        # Collect names of columns in a list
        clist = []
        for child in r_columns_root[0]:
            clist.append(child.tag)

        ghgrp_data = pd.DataFrame(columns=clist)

        if rows is None:
            # Get number of rows of request, if not specified.
            # If rows is None then all rows of the table will be requested.
            try:
                r = requests.get(table_url + '/count/')

            except requests.exceptions.RequestException as e:
                print(e, table_url)
                sys.exit(1)

            else:
                nrecords = int(et.fromstring(r.content)[0].text)

            # Limit to requesting >10,000 records?
            # This could be refactored.
            if nrecords > 10000:
                rrange = range(0, nrecords, 10000)

                # Call
                for n in range(len(rrange) - 1):
                    try:
                        r_records = requests.get(table_url + '/rows/' +
                                                 str(rrange[n]) + ':' +
                                                 str(rrange[n + 1]))

                    except requests.exceptions.RequestException as e:
                        print(e, table_url)
                        sys.exit(1)

                    else:
                        records_root = et.fromstring(r_records.content)
                        r_df = GHGRP_API.xml_to_df(records_root, table,
                                                   ghgrp_data.columns)
                        ghgrp_data = ghgrp_data.append(r_df)

                records_last = requests.get(table_url + '/rows/' +
                                            str(rrange[-1]) + ':' +
                                            str(nrecords))

                records_lroot = et.fromstring(records_last.content)
                rl_df = GHGRP_API.xml_to_df(records_lroot, table,
                                            ghgrp_data.columns)
                ghgrp_data = ghgrp_data.append(rl_df)

            else:
                try:
                    r_records = \
                        requests.get(table_url + '/rows/0:' + str(nrecords))

                except requests.exceptions.RequestException as e:
                    print(e, table_url)
                    sys.exit(1)

                else:
                    records_root = et.fromstring(r_records.content)
                    r_df = GHGRP_API.xml_to_df(records_root, table,
                                               ghgrp_data.columns)
                    ghgrp_data = ghgrp_data.append(r_df)

        else:
            try:
                r_records = requests.get(table_url + '/rows/0:' + str(rows))

            except requests.exceptions.RequestException as e:
                print(e, table_url)
                sys.exit(1)

            else:
                records_root = et.fromstring(r_records.content)
                r_df = GHGRP_API.xml_to_df(records_root, table,
                                           ghgrp_data.columns)
                ghgrp_data = ghgrp_data.append(r_df)

        # Drop any duplicate row entries.
        ghgrp_data.drop_duplicates(inplace=True)

        return ghgrp_data
