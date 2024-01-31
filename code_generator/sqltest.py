from ast import Pass
from logging import root
from pathlib import Path
from posixpath import split
import os
import sys,getopt
from datetime import datetime
from oogway_entities import *
from emworks_templates import stringtemplate
from Util import Util
import io
import zipfile
import subprocess
from oogway_classes import OutputBuilder
#https://pypi.org/project/sqlparse/
import sqlparse
#https://pypi.org/project/sql_metadata/
from sql_metadata import Parser

sql='''
WITH
  _platformcompanyid_to_cdb_id_current AS (
   SELECT
     platformcompanyid
   , externalcompanyid
   , cdb_deleted
   , name
   FROM
     staged_company_repo.company
   WHERE ((ttd = (SELECT max(ttd)
FROM
  staged_company_repo.company
)) AND (externalcompanyidsource = 'CDB'))
) 
, _primary_company_current AS (
   SELECT
     f.platformcompanyid
   , f.platformcompanyid
   , f.renewalemail
   , f.platformfleetorganizationidentifier
   , f.fleetsolution
   , c.externalcompanyid CDB_ID
   FROM
     (staged_company_repo.fleet f
   INNER JOIN _platformcompanyid_to_cdb_id_current c ON (c.platformcompanyid = f.platformcompanyid))
   WHERE (f.ttd = (SELECT max(ttd)
FROM
  staged_company_repo.fleet
))
) 
, _rolename AS (
   SELECT
     name
   , platformroleidentifier
   , staged_created_time
   , timestamp dms_timestamp
   FROM
     staged_permission_service.ps_role
) 
, _florg_odl_fleetuser_current AS (
   SELECT
     fleet_id
   , user_id
   , role_id_csv
   , array_agg(rn.name ORDER BY rn.name ASC) role_names
   , ttd
   FROM
     ((ds_florg.florg_odl_fleetuser fu
   CROSS JOIN UNNEST(split(role_id_csv, ',')) t (role_id))
   INNER JOIN _rolename rn ON (rn.platformroleidentifier = t.role_id))
   WHERE (ttd = (SELECT max(ttd)
FROM
  ds_florg.florg_odl_fleetuser
))
   GROUP BY fleet_id, user_id, role_id_csv, ttd
) 
, _latest_user_authentication AS (
   SELECT
     platformuseridentifier
   , username
   , latest_authentication
   , authentication_type
   FROM
     ds_accountservice.v_accountservice_odl_userauth_latest
) 
, _userinfo AS (
   SELECT
     ui.id user_info_id
   , ui.platformuseridentifier
   , e.emailaddress
   , (CASE WHEN (u.email LIKE '%@wirelesscar.fake') THEN '' ELSE u.email END) email
   , u.firstname
   , u.lastname
   , u.locale
   , u.status
   FROM
     ((staged_user_repo.v_ur_userinfo_latest ui
   LEFT JOIN staged_user_repo.v_ur_users_latest u ON (u.platformuseridentifier = ui.platformuseridentifier))
   LEFT JOIN staged_user_repo.v_ur_email_latest e ON (e.user_info_id = ui.id))
) 
SELECT
  platformfleetorganizationidentifier fleet_id
, f.marketname
, f.fleetname
, cf.CDB_ID primary_cdb_id
, u.firstname
, u.lastname
, u.email
, u.emailaddress
, fu.role_names
, u.locale
, a.latest_authentication
, to_iso8601(current_date) report_created_date
FROM
  ((((_userinfo u
LEFT JOIN _florg_odl_fleetuser_current fu ON (fu.user_id = u.platformuseridentifier))
LEFT JOIN _latest_user_authentication a ON (a.platformuseridentifier = u.platformuseridentifier))
LEFT JOIN ds_florg.v_florg_odl_fleet f ON (f.fleetid = fu.fleet_id))
LEFT JOIN _primary_company_current cf ON (cf.platformfleetorganizationidentifier = fu.fleet_id))
WHERE (any_match(role_names, (x) -> (x = 'Volvo Connect Fleet Administrator')) OR any_match(role_names, (x) -> (x LIKE '%Signatory%')))
'''

tables = Parser(sql).tables

print(tables)