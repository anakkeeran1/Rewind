###Proce:s Logic###
        #Do Logic to run job and get Status
        #If runAJob returns a success code stop Agent
        #Loop: Run get_agentService to ensure agent is stopped
        #Once stopped, replace files
        #Bring agent back up
        #Loop: Run get_agentService to ensure agent is started
        #Spin up new DIS by calling set_DISProp
        #Loop: Run get_agentService to ensure agent is started
        #Once running run job once more to confirm rollback is complete
        #Show stat and processed count

#Imports
import requests
import json
import subprocess
import datetime
import configparser
import cx_Oracle
import shutil
import os
import time


#Pull parameters from param file.

# Create a ConfigParser object
config = configparser.ConfigParser()

# Read the config file
config.read('config.ini')
task_name="test"
connector="connector"
releasename="releasename"

# Access values from specific sections
username = config['settings']['username']
password = config['settings']['password']
loginURL = config['settings']['loginURL']
serviceURL = config['settings']['serviceURL']
runajobclipath = config['settings']['runajobclipath']
folderpath = config['settings']['folderpath']
DB_USER = config['settings']['DB_USER']
DB_PASSWORD = config['settings']['DB_PASSWORD']
DB_HOST = config['settings']['DB_HOST']
DB_PORT = config['settings']['DB_PORT']
DB_SERVICE_NAME = config['settings']['DB_SERVICE_NAME']
backupdir = config['settings']['backupdir']
agentdir = config['settings']['agentdir']
newpackagename = config['settings']['newpackagename']
oldpackagename = config['settings']['oldpackagename']
rollback_path = config['settings']['rollback_path']

dsn = DB_HOST+":"+DB_PORT+"/"+DB_SERVICE_NAME
step = 'test'

timestamp = time.strftime('%Y%m%d%H%M%S', time.localtime())

#Adams Section
#Get Session ID for IICS API Calls.
def getSessionID(url, data=None, headers=None):
    try:
        # Send a POST request to the API endpoint with data and headers
        response = requests.post(url, json=data, headers=headers)

        # Raise an exception if the request was unsuccessful
        response.raise_for_status()

        # Parse the response in JSON format
        response_data = response.json()

        # Extract the sessionId
        session_id = response_data.get('userInfo', {}).get('sessionId')

        # Print the Session ID
        print(f"Session-ID: {session_id}")

        # Return the Session ID
        return session_id
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

#Get the Secure Agent DIS Status.
def get_agent_service(service_url, session_id):
    url = f"{service_url}/api/v2/agent/details/01C7NV0800000000000C"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "icSessionId": session_id
    }

    # Send GET request
    response = requests.get(url, headers=headers)

    # Parse JSON response
    data = response.json()

    # Filter and process the response to find Data_Integration_Server
    dis = [engine for engine in data.get('agentEngines', []) if engine['agentEngineStatus']['appname'] == 'Data_Integration_Server']

    if dis:
        # Find the max version
        dis_version_info = max(dis, key=lambda x: x['agentEngineStatus']['appversion'])
        dis_version = dis_version_info['agentEngineStatus']['appversion']
        dis_top_status = dis_version_info['agentEngineStatus']['status']

        print(f"Status of Data Integration Server, version: {dis_version} is : {dis_top_status}")
        return dis_version, dis_top_status
    else:
        print("No Data Integration Server found")
        # Return a default or placeholder status if no Data Integration Server is found
        return None, "UNKNOWN"

#Set a DIS Property to force a new version spin-up.
def set_DISProp(service_url, session_id):
    url = f"{service_url}/api/v2/runtimeEnvironment/01C7NV25000000000008/configs/linux64"

    # Data to be sent in the PUT request
    data = {
        "Data_Integration_Server": [
            {
                "TOMCAT_CFG": [
                    {
                        "name": "NetworkTimeoutPeriod",
                        "value": "300"
                    }
                ]
            }
        ]
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "icSessionId": session_id
    }

    # Send PUT request
    response = requests.put(url, headers=headers, data=json.dumps(data))

#RunAJob for Taskflow update
def run_a_tf(task_name, runajobclipath):
    print(f"Running Taskflow - '{task_name}'")

    # Generate a unique log file name
    date_str = datetime.datetime.now().strftime('%b%d%Y')
    logname = f"runajob_log_{date_str}.txt"

    # Run the Mapping task using RunAJobCli and check status
    cmd = f"{runajobclipath}/cli.sh runAJobCli -u {username} -p {password} -t TASKFLOW -un {task_name} -d"
    print(cmd)
    with open(logname, "w") as logfile:
        process = subprocess.run(cmd, shell=True, stdout=logfile, stderr=subprocess.STDOUT)
        print(process.returncode)

    # Check status of the runajobcli command output
    if process.returncode == 0:
        job_status = "Success"
        print("Job Succeeded")
    else:
        job_status = "Failed"
        print("Job Failed")

    return job_status



#Loop through both Source/Target Mapping Tasks
def trigger_jobs(tf,runajobclipath):
    task1_status = run_a_tf(tf, runajobclipath)
    if task1_status == "Success":
        print(f"{tf} Completed Successfully")
        if task1_status == "Success":
            trigger_job_status = "Success"
            print("Taskflow completed Successfully")
        else:
            trigger_job_status = "Failed"
            print(f"{tf} Failed.")
    else:
        trigger_job_status = "Failed"
        print(f"{tf} Failed.")

    return trigger_job_status

#Start/Stop the Agent
def start_stop_agent(action):
    agent_core_path = "/home/infaadmin/infaagent/apps/agentcore/"
    rewind_tool_path = "/home/infaadmin/Tools/rewindTool"

    # Change to the agent core directory
    os.chdir(agent_core_path)

    if action == "startup":
        # Start the agent
        subprocess.run(["./infaagent.sh", "startup"])
        # Optional statement post startup
        os.chdir(rewind_tool_path)
        print("Agent is started")
    else:
        # Shutdown the agent
        subprocess.run(["./infaagent.sh", "shutdown"])
        # Changing directory after shutdown as per original logic
        os.chdir(rewind_tool_path)

def agent_loop(action, service_url, session_id):
    """
    Manages the startup or shutdown loop for the Secure Agent until the desired status is achieved.

    Args:
        action (str): 'startup' or 'shutdown'.
        service_url (str): Base URL of the service.
        session_id (str): The current session ID for API requests.
    """
    print(f"{action.capitalize()}ing Agent...")

    # Start or stop the agent based on the action
    start_stop_agent(action)

    # Expected status based on the action
    expected_status = "RUNNING" if action == "startup" else "STOPPED"

    # Poll the agent service until the desired status is reached
    _, current_status = get_agent_service(service_url, session_id)
    while current_status != expected_status:
        print(f"Current Agent Status: {current_status}")
        time.sleep(10)  # Wait before checking status again
        _, current_status = get_agent_service(service_url, session_id)

    print(f"Agent {action.capitalize()} done!")

#Arun's Section

def get_tf_from_db(connector):
    #Get Taskflow name from database
    # Connect to the Oracle database
    connection = cx_Oracle.connect(DB_USER, DB_PASSWORD,dsn )

    # Create a cursor
    cursor = connection.cursor()

    # Define the query to select specific columns
    query = 'SELECT TASKFLOW_NAME FROM REWIND_CONFIG_TF WHERE connector = :connector'

    # Execute the query with the user-provided filter value
    cursor.execute(query, connector=connector)
    result = cursor.fetchall()
    tf_temp = json.dumps(result)
    tf_temp = str(tf_temp)
    tf = tf_temp.replace('[["', '').replace('"]]', '')

    return tf


def audit_log_entry(connector,step,username):
    # Establish the database connection
    connection = cx_Oracle.connect(DB_USER, DB_PASSWORD, dsn)
    cursor = connection.cursor()
    # SQL query to insert a row
    insert_query = """ INSERT INTO rewind_audit (connector, step, username) VALUES (:1, :2, :3) """
    data = (connector, step, username)
    # Execute the query and commit the changes
    cursor.execute(insert_query, data)
    connection.commit()
    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Row inserted successfully.")

def process_command(command):
#    command='copy all from /home/infaadmin/Tools/rewindTool/agentdir/downloads/newpackage to /home/infaadmin/Tools/rewindTool/backup/newpackage'

    audit_log_entry(connector,command,username)
    words = command.split()  # Split the string by whitespace into a list of words
    first_three_words = words[:3]  # Get the first three words
    lower_first_three_words = [word.lower() for word in first_three_words]  # Convert each word to lowercase

    if lower_first_three_words == ["copy", "all", "from"]:
        src = words[3:4]
        dest = words[5:6]
        source = "".join(src)
        destination = "".join(dest)

        # If destination directory exists, delete it before copying
        print(step+' Started')
        if os.path.exists(destination):
            shutil.rmtree(destination)
            shutil.copytree(source, destination)
        else:
            shutil.copytree(source, destination)
        print(step+' completed')
    elif lower_first_three_words == ["copy", "file", "from"]:
        src = words[3:4]
        dest = words[5:6]
        source = "".join(src)
        destination = "".join(dest)
        shutil.copy(source, destination)
    elif lower_first_three_words == ["remove", "file", "from"]:
        src = words[3:4]
        source = "".join(src)
        if os.path.exists(source):
            os.remove(source)
            print(f"File {source} has been removed.")
    elif lower_first_three_words == ["remove", "all", "from"]:
        src = words[3:4]
        source = "".join(src)
        folder_path = source

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path) # Remove the file or symbolic link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path) # Remove the directory and its contents
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
                audit_log_entry(connector,'Failed to delete {file_path}. Reason: {e}',username)
        print(f"All contents of the folder {folder_path} have been removed.")
    else:
        print("end if")

def validate_steps(connector):
    # Connect to the Oracle database
    connection = cx_Oracle.connect(DB_USER, DB_PASSWORD,dsn )

    # Create a cursor
    cursor = connection.cursor()

    # Define the query to select specific columns
    query = 'SELECT STEP_NAME,STEPS FROM REWIND_CONFIG WHERE connector = :connector'

    # Execute the query with the user-provided filter value
    cursor.execute(query, connector=connector)

    for row in cursor:
        #print(f"STEP_NAME: {row[0]}, STEPS: {row[1]}")
        step="".join({row[1]})
        step_rep = step.replace("<agentdir>",agentdir).replace("<newpackagename>",newpackagename).replace("<backupdir>",backupdir).replace("<rollback_path>",rollback_path).replace("<oldpackagename>",oldpackagename)
        print("Step:",step_rep)
        print('------')

    # Close the cursor and connection
    cursor.close()


    # Get user input for the validation
    validate = input("Verify the steps and confirm to proceed (Y/N):  ")

    if validate == 'N':
        print('Steps are invalid. Please recheck. Exiting..')
        exit()


def process_rollback(connector):
    # Connect to the Oracle database
    connection = cx_Oracle.connect(DB_USER, DB_PASSWORD,dsn )

    # Create a cursor
    cursor = connection.cursor()

    # Define the query to select specific columns
    query = 'SELECT STEP_NAME,STEPS FROM REWIND_CONFIG WHERE connector = :connector'

    # Execute the query with the user-provided filter value
    cursor.execute(query, connector=connector)

    for row in cursor:
        print(f"STEP_NAME: {row[0]}, STEPS: {row[1]}")
        step="".join({row[1]})
        step_rep = step.replace("<agentdir>",agentdir).replace("<newpackagename>",newpackagename).replace("<backupdir>",backupdir).replace("<rollback_path>",rollback_path).replace("<oldpackagename>",oldpackagename)
        print("step**:",step_rep)
        #call function to execute the steps
        process_command(step_rep)

    # Close the cursor and connection
    cursor.close()

def replace_string(identifier,newvalue):
    # File path
    file_path = "config.ini"

    # The identifier and the new value
    identifier = identifier
    new_value = newvalue

    # Read the file and modify the value for the identifier
    with open(file_path, "r") as file:
        lines = file.readlines()

    # Search and replace the value for the identifier
    for i in range(len(lines)):
        if identifier in lines[i]:
            key, _ = lines[i].split("=")  # Splits at the "="
            lines[i] = f"{key}={new_value}\n"  # Updates the value
            break
    # Write the updated lines back to the file
    with open(file_path, "w") as file:
        file.writelines(lines)

    print("Value replaced successfully!")


def rollback(connector, session_id, service_url, runajobclipath, folderpath):
    """
    Manages the rollback process, including stopping and restarting the agent, and running required jobs.

    Args:
        connector (str): The connector to be rolled back.
        session_id (str): Current session ID for API requests.
        service_url (str): Base URL of the service.
        source_mtt (str): Source mapping task name.
        target_mtt (str): Target mapping task name.
        runajobclipath (str): Path to the RunAJobCli.
        folderpath (str): Folder path for the tasks.
    """
    print("Starting rollback process...")
    # Rollback start and timestamp to audit

    # Logic to get the taskflow name from db(Arun)
    tf = get_tf_from_db(connector)
    print(tf)

    # Get initial agent service status
    _, current_status = get_agent_service(service_url, session_id)

    while current_status != "RUNNING":
        print(f"Agent Status = {current_status}. Please ensure the agent is running and configured correctly.")
        retry = input("Press 'y' to retry or 'n' to end the job: ").lower()
        if retry == 'n':
            print("Exiting logic - Agent is not running.")
            return
        _, current_status = get_agent_service(service_url, session_id)

    # Trigger initial jobs
    trigger_job_status = trigger_jobs(tf, runajobclipath)

    while trigger_job_status != "Success":
        print("Trigger jobs failed. Check the session log for details.")
        retry = input("Press 'y' to retry or 'n' to end the job: ").lower()
        if retry == 'n':
            print("Exiting logic - Trigger jobs failed.")
            return
        trigger_job_status = trigger_jobs(tf, runajobclipath)


    #validate the rollback steps
    validate_steps(connector)

    print("Stopping Secure Agent...")
    # Stop the agent
    agent_loop("shutdown", service_url, session_id)

    # Placeholder for rollback steps
    #validate_steps(connector)
    # Audit: User's input
    process_rollback(connector)
    # Audit: Rollback steps

    print("Starting Secure Agent...")
    # Start the agent
    agent_loop("startup", service_url, session_id)

    print("Setting DIS properties...")
    # Set DIS properties
    set_DISProp(service_url, session_id)

    # Ensure DIS is running
    _, current_status = get_agent_service(service_url, session_id)
    while current_status != "RUNNING":
        print(f"Current Agent Status: {current_status}. Please ensure DIS is configured correctly.")
        time.sleep(10)
        _, current_status = get_agent_service(service_url, session_id)

    print("New DIS Started!")

    # Trigger final jobs
    final_trigger_status = trigger_jobs(tf, runajobclipath)
    while final_trigger_status != "Success":
        print("Final job(s) failed after rollback. Check the session log for details.")
        retry = input("Press 'y' to retry or 'n' to end the job: ").lower()
        if retry == 'n':
            print("Exiting logic - Final jobs failed.")
            return
        final_trigger_status = trigger_jobs(tf, runajobclipath)

    print("All jobs completed successfully!")

api_url = f'{loginURL}/saas/public/core/v3/login'
data = {'username': f'{username}', 'password': f'{password}'}
headers = {'Content-Type': 'application/json'}
sessionID = getSessionID(api_url, data, headers)


# Get user input for the filter
connector = input("Enter the Connector Name:  ")
runninguser = input("Enter the user name: ")
oldpackagename = input("Enter the oldpackage name:")
replace_string("oldpackagename=",oldpackagename)
newpackagename = input("Enter the newpackage name:")
replace_string("newpackagename=",newpackagename)

rollback(
    connector=connector,
    session_id=sessionID,
    service_url=serviceURL,
    runajobclipath=runajobclipath,
    folderpath=folderpath
)

