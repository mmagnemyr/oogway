from ast import Pass
from logging import root
from pathlib import Path
from posixpath import split
import os
import sys,getopt
from datetime import datetime
from oogway_entities import *
from emworks_templates import stringtemplate
from pathlib import Path
from posixpath import split
import io
import sys,getopt
from datetime import datetime
import inspect
from asyncio.log import logger

SUCCESS=0
INFO=1
HINT=2
WARNING=3
ERROR=4
SOLUTIONERROR=5
DEBUG1=6
DEBUG2=7
DEBUG3=8




class Util:
    _instance = None
    enable_debug_messages=False
    exit_on_errors=True
    debug_level=1

    @classmethod
    def copyTree (cls,src,trg):
        cls.print(DEBUG1,"Copy Tree from ", src, " to ", trg)
        if os.path.exists(trg)==False:
            shutil.copytree(src,trg, dirs_exist_ok=True)
        else:
            Util.clearDir(trg)
            shutil.copytree(src,trg,dirs_exist_ok= True)

    @classmethod
    def print_table(cls, headers, rows, column_width=42):
        # Print headers
        header_line = " | ".join(f"{header:<{column_width}}" for header in headers)
        print("")
        print(header_line)
        print("-" * (len(header_line) + len(headers) - 1))

        # Print rows
        for row in rows:
            formatted_row = []
            for attr in headers:
                value = getattr(row, attr)
                if isinstance(value, os.DirEntry) and hasattr(value, 'name'):
                    formatted_row.append(f"{value.name:<{column_width}}")
                else:
                    formatted_row.append(f"{str(value):<{column_width}}")
            row_line = " | ".join(formatted_row)
            print(row_line)
        print("")

    @classmethod
    def getCurrentDate(cls):
        return datetime.now().strftime("%Y-%m-%d")

    @classmethod
    def debug_sysexit(cls):
        if cls.enable_debug_messages==True:
            caller_frame = inspect.currentframe().f_back
            file_name = caller_frame.f_code.co_filename
            line_number = caller_frame.f_lineno
            #print(f"Method called at {file_name}:{line_number}")

            # Create a JSON string with the file and line information
            location_info = json.dumps({"file": file_name, "line": line_number})
            
            
            cls.print(WARNING,"A developer has forced oogway.py to stop here... Consider removing call to Util.debug_sysexit() in production systems. However, this exit will only happen if debug is enabled with option -d.")
            cls.print(WARNING,f"{location_info}")
        
        sys.exit(1)

    @classmethod
    def getopt(cls,*args, **kwargs):
        try:
            return getopt.getopt(*args, **kwargs)
        except getopt.GetoptError as e:
            cls.print(ERROR,"Sorry, you might have given options that is currently not supported! Please check to error message:", e)
            sys.exit()
   
    @classmethod
    def enableDebugLevel(cls,dl):
       
        if int(dl)==0:
            
            cls.enable_debug_messages=False
        else:
            cls.enable_debug_messages=True
            debug_level=dl


    def clearDir(p):
        files = glob.glob(os.path.join(p,'*'))
        for f in files:
            if not os.path.isdir(f) and not '.xlsxa' in f:
                try:
                    os.remove(f)
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))

    @classmethod
    def mkdir(cls,p):
        try:
            isExists = os.path.exists(p)
            if not isExists:
                os.makedirs(p)
        except PermissionError as e:
            cls.print(ERROR, "Seems like we do not have permission to use ", p, " Please check if the path given as -p was corrent.")
            sys.exit(1)
        except Exception as e:
            cls.print(ERROR, "The following exception occured when trying to create the folede ", p, ":", e)        
    
    def intersection(list1, list2):
        lst3 = [value for value in list1 if value in list2]
        return lst3
    @classmethod
    def print(cls,message_type,*args, **kwargs):

        location_info=""

        if message_type==DEBUG1 or message_type==DEBUG2 or message_type==DEBUG3:
            if cls.enable_debug_messages==False:
                return
            elif cls.debug_level==1 and (message_type==DEBUG2 or message_type==DEBUG3):
                return

        

        message = ' '.join(map(str, args))  # Convert arguments to a string
        sep = kwargs.get('sep', ' ')
        end = kwargs.get('end', '\n')

        text_color = '\033[0m'

        mt="INFO"
        if message_type==HINT:
            mt="HINT"
            text_color="\033[92m"

        elif message_type==SUCCESS:
            text_color="\033[92m"
            mt="SUCCESS"
            

        elif message_type==ERROR:
            caller_frame = inspect.currentframe().f_back.f_back
            
            file_name = caller_frame.f_code.co_filename
            line_number = caller_frame.f_lineno
            # Create a JSON string with the file and line information
            location_info = json.dumps({"file": file_name, "line": line_number})
            text_color="\033[91m"
            mt="ERROR"
        elif message_type==SOLUTIONERROR:
            text_color="\033[91m"
            mt="MODEL"

        elif message_type==WARNING:
            text_color = "\033[93m"
            mt="WARN"
        elif message_type==DEBUG1 or message_type==DEBUG1 or message_type==DEBUG3:
            text_color = "\033[94m"
            mt="DEBUG"

        formatted_message = f"[{mt}] {message}"
        formatted_message=formatted_message.replace("[:yellow]", '\033[93m')
        formatted_message=formatted_message.replace("[:default]", text_color)

        print(f"{text_color}{formatted_message}\033[0m", sep=sep, end=end)
        if message_type==ERROR:
            
            print(location_info)

        if message_type==SOLUTIONERROR:
            sys.exit(1)
        elif message_type==ERROR and cls.exit_on_errors:
            sys.exit(1)

    @classmethod
    def printLine(cls):
        print("-------------------------------------------------------------")
    
    @classmethod
    def copy(cls,src,trg):
        try:
            cls.print(DEBUG1, "Copy ", src, " to ", trg)
            shutil.copy(src,trg)
        except PermissionError as e:
            Util.print(ERROR,"There was a problem opening the file ", e.filename,  ". It could be that you have it opened in Excel or someone else is using that file. Also check in task manager if there is a process named EXCEL.EXE in the Details Tab.  This script will exit.")
           
            sys.exit(1)
        except Exception as e:
            Util.print(ERROR, e)
            sys.exit(1)