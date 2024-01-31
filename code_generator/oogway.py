from ast import Pass
from logging import root
from pathlib import Path
from posixpath import split
import os
import sys,getopt
from datetime import datetime
from oogway_entities import *
from emworks_templates import stringtemplate
from Util import Util, SUCCESS,ERROR,INFO,WARNING,DEBUG1,DEBUG2,DEBUG3, SOLUTIONERROR,HINT
from difflib import get_close_matches
import io
import re
import zipfile
import subprocess
from oogway_classes import OutputBuilder, OogwayTable, OogwayFun, OogwayConfig
#https://pypi.org/project/sqlparse/
import sqlparse
#https://pypi.org/project/sql_metadata/
from sql_metadata import Parser

"""
options
    the options is described in the text file ./help/oogway_help.txt
 
"""

def main(argv):
    version=122
    OogwayFun.welcome()
    oogway_config = OogwayConfig.load()
   
    

    opt=""
    action=""
    action_list = ["build", "init", "backup", "upgrade", "useupgrade","refresh", "scan", "list"]
    solution_name=""
    
    table_to_add="new_table.xlsx"
    set_deploy_to_to="cdk"
    
    clear_folders=False
    solution_paths= SolutionPaths()
    
    file_to_process="all"
    output={}

    short_options="p:a:f:t:n:d:h:"
    long_options=["env=","path="]

    """Read the command line arguments...."""
    opts,args = Util.getopt(argv,short_options, long_options)
    for opt,arg in opts:    
        if opt=='-p':
            solution_paths.set_root_folder( arg)
            
        elif opt=='-a':
            action=arg

        elif opt=='-f':
            file_to_process=arg
        
        elif opt=='-t':
            table_to_add=arg
        elif opt=='-n': #solution name
            solution_name=arg
        elif opt=='-d':
            
            Util.enableDebugLevel(arg)
        elif opt=='-h':
            OogwayFun.printHelp()
            return
        elif opt=='--env':
            set_deploy_to_to=arg

    Util.print(DEBUG1,"opt",opt, "action",action)    
    if opt=="" and action=="":
        OogwayFun.printHelp()
        sys.exit()

    if solution_paths.root_folder=="":
        Util.print(HINT, "Oogway kindly requests for some more info...")
        Util.print(HINT,"\tArgument -p can not be left out! -p is the path to your solution folder.")
        Util.print(ERROR,"FAILED. Please fix and try again!")
        sys.exit(1)

    if file_to_process!="all" or action=="upgrade" or action=="backup" or action=="refresh" or action=="useupgrade":
        clear_folders=False
    
    
    #solution_paths.latest_oogway_version=oogway_config.oogway_templates_version
    Util.print(INFO,"You are using Oogway version: ", str(version))
    #if version!=oogway_config.oogway_templates_version:
        #Util.print(WARNING,"\tYou are currently using Oggway Excel Templates Version:",str(oogway_config.oogway_templates_version))

    templates=stringtemplate
    docontinue=True
    
    dvb = OogwayCodeBuilder()
    cdk = CDKHelper    

    generic_steps_created:bool=False
    main_solution_file=None
    htmlfile=""
    dim=None
    backup_path=""

    """Alright, what action does the user want to execute?"""
    if action=="init":
        solution_paths.createfolders(clear_folders)
        solution_paths.init_solution(solution_name, refresh_only=False)
        Util.print(SUCCESS,"Your Solution folder is now ready! Thank tou for using Ooogway!")
        return
    elif action=="refresh":
        solution_paths.createfolders(clear_folders)
        Util.print(INFO,"Refreshing Oogway scripts and documentation..")
        solution_paths.init_solution(solution_name, refresh_only=True)
        Util.print(SUCCESS, "Thank you for using Oogway!")
        return 
    elif action=="add":
        Util.print(INFO,"Adding table ", table_to_add, " to your solution.")
    elif action=="list":
        pass
        
    elif action=="useupgrade":
        Util.print(INFO,"Moves your current files in the database folder to a backup folder and replace them with the upgraded files!")
        solution_paths.useupgrade()
        return   
    elif action=="backup":
        Util.print(INFO,"Backup is always a wise choice..")
        solution_paths.backup()
        sys.exit(1)
    elif action=="upgrade":
        Util.print(INFO,"Upgrading your files in database to the latest version. You might want to check for new features in them.")
        solution_paths.backup()
        
    elif action=="build":
        pass
    else:
        s=""
        matches = get_close_matches(action, action_list)
        if matches:
            suggestion=matches[0]
            s=" Did you mean: " + suggestion + "?"
        Util.print(ERROR,"Sorry, unknown action was given in argument -a: ", action,".", s )
        sys.exit(1)
    
    list_of_tables_to_build=[]
    list_of_tables_to_build_2=[]

    list_of_dim_tables=[]
    
    """Check Paths... this should be moved into SolutionPaths"""
    if os.path.exists(solution_paths.excel_input_folder)==False:
        Util.print(HINT,"The database directory does not seem to exist in your solution folder. If this is a new solution then you might want to initialize it first?")
        Util.print(HINT, "Run: py ./oogway.py -p ", solution_paths.root_folder, " -a init -n [name of your solution]")
        Util.print(ERROR,"FAILED. Please fix and try again!")
        sys.exit(1)
    if os.path.exists(solution_paths.solution_file_full_path)==False:
        Util.print(HINT,"There is no solution.xlsx in the folder " , solution_paths.excel_input_folder + " If this is a new solution then you might want to initialize it first?")
        Util.print(ERROR,"FAILED. Please fix and try again!")
        sys.exit(1)
    if os.path.exists(solution_paths.oogway_build_path)==False:
        Util.print(HINT,"There are some missing sub folders in you solution.")
        Util.print(HINT,"If you just did a git pull then that might be the reason that the oogway folder is missing.")
        Util.print(HINT,"(It's generally a good idea to keep the oogway folder in the .gitignore file.)")
        Util.print(HINT,"To solve this please run the follwing command:")
        Util.print(HINT, "\tpy ./oogway.py -p ", solution_paths.root_folder, " -a refresh")
        Util.print(HINT, "If you are in the powershell_helper_scripts folder you can use the script:")
        Util.print(HINT, "\t./oogway_refresh.ps1")
        Util.print(ERROR,"FAILED. Please fix and try again!")
        sys.exit(1)

    """Ready to go!
        1.  First we need to read the solution.xlsx file. T
            This procedure will also set deploy_to to cdk by default. 
            if you want another option for deploy_to please use the --env option i.e --env prod
    """

    for f in os.scandir(solution_paths.excel_input_folder): 
        #f is a DirEntry
        if "~$" != f.name[0:2] and ".xlsx" == f.name[-5:]:
            if f.name.find("solution")>-1:
                main_solution_file = DvDim(f)
                
                #sets the deploy_to attribute in solution.xlsx to cdk by default or use --env prod or --env qa
                main_solution_file.set_deploy_to(solution_paths,set_deploy_to_to,action)

                Util.print(INFO, "Reading Main solution file ", solution_paths.solutionfile_build_fullpath)
                main_solution_file.fullpath=solution_paths.solutionfile_build_fullpath
                main_solution_file.readMainSolution(solution_paths, version, action)
                solution_paths.setCode(main_solution_file.output_builder)
                
    #2. next we create a list with all tables to build
    for file2 in os.scandir(solution_paths.excel_input_folder):
        ext = file2.name[-5:]   
        if "~$" != file2.name[0:2] and ".xlsx" == ext:            
            if file2.name.find("_stub")>-1:
                pass
            if file2.name.find("solution")>-1:
                pass 
            else:           
                Util.print(DEBUG1,"Open file ", file2.name)
                dim=DvDim(file2)
                
                if dim.is_valid:
                    ootable = OogwayTable()

                    try:    
                        Util.print(INFO, "Table Name:", dim.table_name)
                        ootable.table_name= main_solution_file.target_database + "." +dim.table_name
                        
                        ootable.file=file2
                        ootable.powershell_edit="./edit.ps1 " +  os.path.splitext(ootable.file.name)[0]
                       

                        if ootable.table_name in main_solution_file.list_of_view_names:
                            Util.print(SOLUTIONERROR,"\tThe solution contains a view with the same name as this table!", ootable.table_name, " This will not work, either rename the view or the table!")
            
                        if not any(obj.table_name == ootable.table_name for obj in list_of_tables_to_build_2):
                            list_of_tables_to_build.append(ootable.table_name)
                            list_of_tables_to_build_2.append(ootable)
                        else:
                            found_item = next((obj for obj in list_of_tables_to_build_2 if obj.table_name == ootable.table_name), None)
                            Util.print(SOLUTIONERROR,"The table name " + ootable.table_name + " in file ", ootable.file.name, " is already defined in file ", found_item.file.name + ". Please correct this.")
                            sys.exit(1)

                    except Exception as e:
                        Util.print(ERROR, "Loading file:", f.path, " did not work!, Please check the Excel file, sometimes they does not work properly.",e )                
                        sys.exit(1)
    
    number_of_tables_to_process= len(list_of_tables_to_build_2)

    if action=="list":
        if number_of_tables_to_process>0:
            Util.print(INFO,"A list of all tables in your solution:")
            headers = ["file","table_name", "powershell_edit"]
            Util.print_table(headers, list_of_tables_to_build_2)
           
            Util.print(HINT,"You can copy/paste the powershell_edit command to edit the table in Excel!" )
        else:
            Util.print(INFO,"So far there are no tables in your solution. Or did you move them somewhere else?" )

        Util.print(SUCCESS, "Thank you for using Oogway! Happy Coding...")
        sys.exit(1)

    if action=="add":
        #now add a the new table to the solution if a table with that name does not already exist
        if table_to_add not in list_of_tables_to_build_2:
            solution_paths.add_table(table_to_add)
            Util.print(SUCCESS,"table successfully added to your solution.")
        else:
            Util.print(SOLUTIONERROR,"A table/view with that name already exists in the solution.")

        sys.exit(1)

   
    if number_of_tables_to_process==0:
        Util.print(INFO,"There are no tables in this use case.")
    else:
        Util.print(INFO,"Number of tables to process:",str(number_of_tables_to_process))

    if action=="upgrade":
        main_solution_file.upgradeMainSolutionFile(solution_paths)

    for oTbl in list_of_tables_to_build_2:
        ext = oTbl.file.name[-5:] 
        name = oTbl.file.name

        if(file_to_process=="all" or name==file_to_process):
            docontinue=True
        else:
            docontinue=False

        if docontinue:
            dim=DvDim(oTbl.file)
            
            if action=="build":
               
                dim.debugPrint()
                
                dim.build(solution_paths,name, main_solution_file)
                list_of_dim_tables.append(dim)

                Util.print(DEBUG1, "\tDependency Analysis...")
                #dim.depchan will contain a list of other tables in the same use case.
                depchain=[]
                
                depchain = Util.intersection(dim.dep_tables,list_of_tables_to_build)  
                dim.target_table_dependencies = depchain
               
                if(len(dim.target_table_dependencies)>0) :
                    Util.print(DEBUG1, "\tDEPENDENCY Detected to the following tables:",dim.target_table_dependencies)
                else:
                    Util.print(DEBUG1, "\tNo Dependencies to other use case tables detected.")
                
                load_method = dim.load_method

                Util.print(INFO,"\tCreating ", main_solution_file.output_builder.orchestration ," and documentation. Load method:", load_method)
                dim.output_code = dvb.create_code(dim,solution_paths, main_solution_file)                      
                
                htmlfile = htmlfile +  "<tr><td><a href='" +  dim.output_code.html_table_filename + "'>" +dim.config["REPLACE_TARGET_TABLE_NAME"] + "</a></td><td><a href='templates.html'> " + str(dim.load_method) + "</a></td><td><a href='" + dim.output_code.htnl_code_filename + "'>" + dim.output_code.platform_resource_identifier + "</a></td><td>" + oTbl.file.name + "</td></tr>"
                
                if dim.load_method==1 or dim.load_method==2 or dim.load_method==3: #dim tables
                    pass #Todo: why do we need this if????
                else: 
                    dvb.html_other_docs.append(dim.doc_hmtl_filename)

            elif action=="upgrade":   
            
                if name=="solution.xlsx":
                    Util.print(INFO, "Upgrading Main Solition File") 
                    main_solution_file.upgradeMainSolutionFile(solution_paths)
                   
                else:
                    Util.print(INFO, "Upgrading table  ", name) 
                    dim.upgrade(solution_paths,name)

    if action=="upgrade":
        print("Finally copy files from ugrade to database")
        #Util.clearDir(solution_paths.excel_input_folder)
        #shutil.copytree (backup_path,solution_paths.excel_input_folder)
        Util.print(HINT, "Your upgraded files can be found in the folder " + solution_paths.oogway_upgrade_path,". Please review them and then use action -a useupgrade to replace your current files in the database folder.")

    if action=="build" and file_to_process=="all":
        
        if(dim!=None):
            dvb.prepare_jq_array_steps(dim,solution_paths, main_solution_file)

        #html table of dependency tables
        htmlfile="<table><th>Table</th><th>Load Method</th><th>Platform Resource Identifier</td><th>Meta Data File</th>" + htmlfile + "</table>"
        dep_table="<table><th>Table Name</th><th>Group</th><th>Dependencies to Use Case Tables</th><th>Other Source Tables</th>"
        dep_table_row=""
        
        for x in list_of_dim_tables:
            x.table_name=x.target_database + "." +  x.table_name

        # Calculate levels for each table
        _ = [calculate_level(dvdim.table_name, list_of_dim_tables) for dvdim in list_of_dim_tables]

        all_table_as_comma_separated_string=""

        # Prepare the dependency table and the main orchestration script
        for dvdim in list_of_dim_tables:
            #convert to comma separated string
            uc_dep = ', '.join(dvdim.target_table_dependencies)
            #remove items from other dependencies so that we do not repeat our self.
            o_dep = [i for i in dvdim.dep_tables if i not in dvdim.target_table_dependencies]
            #convert to comma separated string
            other_dep=', '.join(o_dep)
            all_table_as_comma_separated_string=all_table_as_comma_separated_string+other_dep + ", "
            other_dep=', '.join(o_dep)
            #html table for reporting
            dep_table_row=dep_table_row + "<tr><td>" + dvdim.table_name + "</td><td>" + str(dvdim.level)  + "</td><td>" + uc_dep + "</td><td>"+ other_dep +"</td></tr>"

        dep_table=dep_table+dep_table_row + '</table>'
        main_solution_file.config["REPLACE_DEP_TABLE"] = dep_table   

        tmp = ", ".join(main_solution_file.dep_tables_views)
      
        all_table_as_comma_separated_string=all_table_as_comma_separated_string+"," + tmp
        #extract unique database names
        
        #Use regular expression to find all fully qualified table names
        matches = re.findall(r'\b\w+\.\w+\b', all_table_as_comma_separated_string)
        # Extract unique database names
        unique_db_names = set(match.split('.')[0] for match in matches)
        unique_db_names_string = ', '.join(unique_db_names)
       
        
        if main_solution_file.target_database in unique_db_names:
            unique_db_names.remove(main_solution_file.target_database)
        l2 = '"{}"'.format('","'.join(unique_db_names))
        l3 = '{}'.format(','.join(unique_db_names))
        
        main_solution_file.aws_cloud.aws_db_dependency_list=l2
        main_solution_file.config["REPLACE_DEPENDENCY_DATABASE_LIST"]=l3

        Util.print(DEBUG3,unique_db_names)

        #fix main orchetration... so that tables are loaded in the correct order...
        for x in list_of_dim_tables:
            #print("Table name",x.table_name, ", level", str(x.level))                
            try:
                dvb.other_tables_step_state_machines[x.level].append(x.output_code.platform_resource_identifier)       
            except Exception as e:
                Util.print (ERROR, "Table ", x.table_name + " has  level: " ,str(x.level))
                Util.print (ERROR, e)
 
        main_solution_file = dvb.prepare_main_script(main_solution_file
                                                     , solution_paths
                                                     , dvb.other_tables_step_state_machines
                                                     , htmlfile)
    

    main_solution_file.reset_deploy_to

    #prepare CDK Files

    if action=="build" or action=="deploy":
        Util.printLine()
        Util.print(INFO, "Prepare CDK Project...")
        Util.print(DEBUG1, "CDK: Target Database:", main_solution_file.target_database)

        cdk=CDKHelper(solution_paths.root_folder, solution_paths.template_cdk_fullpath, main_solution_file)
        cdk.copyToDeploy()
        Util.print(SUCCESS, "Thank you for using Oogway! Now try to run the powershell script ./doc.ps1 to view the generated documentation.")
    else:
        Util.print(SUCCESS, "Thank you for using Oogway!")

def calculate_level(table_name, list_of_dim_tables):
    # Find the table in the list_of_dim_tables
    table_obj = next((item for item in list_of_dim_tables if isinstance(item, DvDim) and item.table_name.strip() == table_name.strip()), None)

    if not table_obj or not getattr(table_obj, 'target_table_dependencies', []):
        return 0  # If the list of parents is blank, it is level 0

    # Recursive call to calculate the level of the parent and set the level attribute
    table_obj.level = max([calculate_level(parent, list_of_dim_tables) for parent in getattr(table_obj, 'target_table_dependencies', [])]) + 1
    return table_obj.level

def openSolutionConfigFile(solution_paths):
    filename =solution_paths.root_folder
    
def backup():
    print ("NIY")
    with zipfile.ZipFile("sample.zip",mode='w') as archive:
        archive.printdir()

def upgrade(self, solution_paths:SolutionPaths, name: string):
        target_file = name + "_" + solution_paths.latest_oogway_version + ".xlsx"
        target_fullpath = os.path.join(solution_paths.upgrade_temp_folder,target_file)
        if (self.version==solution_paths.latest_kimball_template_version):
            Util.print(INFO, "\tSkipping: ", self.fullpath.name, ": already latest version")
        else:
            
            file = solution_paths.kimball_template_full_path
            Util.print(INFO, "\tCopying :",file, "to: ", target_fullpath )
            Util.copy(file,target_fullpath)

            #sheet: options
            #sheet: doc
            #sheet: custom source_view
            #sheet: columns
            #sheet: dimensions


            #move old file to old
global logger
#MAIN
if __name__ == "__main__":
    main(sys.argv[1:])


