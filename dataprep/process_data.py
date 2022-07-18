
from pickle import NONE, TRUE
import sys
import os
import logging
#sys.path.append(os.path.join(os.path.dirname("__file__"),'../../'))
from lib.dataprep_etl_utils import run_dataprep
from lib.slack import send_slack_message


import argparse

def main():
    parser = argparse.ArgumentParser(
        prog=os.path.basename(sys.argv[0]), description="Run etl process"
    )
    parser.add_argument(
        "-s",
        "--date_start",
        type=str,
        help="Date Start"
    )
    parser.add_argument(
        "-e",
        "--date_end",
        type=str,
        default="",
        help="Date End"
    ) 
    parser.add_argument(
        "-p",
        "--project",
        type=str,
        default="",
        help="Project Name"
    ) 

    
    args =  parser.parse_args()
   
    try:
        run_dataprep(args = args )
        send_slack_message(args.project, True)
    except Exception as e:
        print('error')
        send_slack_message(args.project, False, error_info=str(e))
   
    

if __name__ == "__main__":
    sys.exit(main())
    