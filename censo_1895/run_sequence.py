import subprocess
import os
import traceback
import datetime

# Define your scripts in the order they should run
scripts = [
    "1_script_censo\script_format.py",
    "2_script_limpieza\script_limpieza.py",
    "3_script_calculo\script_calculo.py",
    "4_script_suma\script_suma.py"
]

# Log file path
log_file_path = "log.txt"

# Function to log errors
def log_error(script, error_message):
    with open(log_file_path, "a") as log_file:
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"{time_now}\t{script}\t{error_message}\n")

# Run scripts in sequence
for script in scripts:
    try:
        # Run the script
        subprocess.run(["python", script], check=True)
        print(f"{script} executed successfully.")

    except subprocess.CalledProcessError as e:
        # If an error occurs during execution, log it
        error_message = f"Script failed with return code {e.returncode}"
        print(f"Error running {script}: {error_message}")
        log_error(script, error_message)

    except Exception as e:
        # Handle any unexpected errors and log them
        error_message = f"Unexpected error: {traceback.format_exc()}"
        print(f"Unexpected error in {script}: {error_message}")
        log_error(script, error_message)
