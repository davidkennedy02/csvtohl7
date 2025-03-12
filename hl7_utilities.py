from datetime import date, datetime
import time  # Add time module for sleep functionality
from pathlib import Path
from hl7apy import core
import hl7apy.core
from logger import AppLogger
from segments import create_pid, create_msh, create_evn, create_pv1
import patientinfo
import multiprocessing

# Replace the global variable with a shared counter
sequence_counter = multiprocessing.Value('i', 0)
logger = AppLogger()

def create_control_id():
    """Creates a unique control ID for the HL7 MSH segment.

    Returns:
        str: A unique control ID.
    """
    current_date_time = datetime.now()
    formatted_date_minutes_milliseconds = current_date_time.strftime("%Y%m%d%H%M%S.%f")
    control_id = formatted_date_minutes_milliseconds.replace(".", "")
    return control_id


def create_message_header(messageType):
    """Creates an HL7 MSH segment and returns the HL7 message.

    Args:
        messageType (str): The type of HL7 message to create.

    Returns:
        hl7apy.core.Message: The HL7 message with the MSH segment.
    """
    current_date = date.today()
    control_id = create_control_id()

    try:
        if messageType == "ADT^A01":
            hl7 = core.Message("ADT_A01", version="2.4")
        else:
            hl7 = core.Message(version="2.4")
        hl7 = create_msh.create_msh(messageType, control_id, hl7, current_date)
        return hl7
    except Exception as e:
        hl7 = None
        logger.log(f"Error creating message header: {e}", "CRITICAL")
        return None
        


def create_adt_message(patient_info:patientinfo.Patient, event_type:str="A01"):
    """Creates an HL7 ADT message with the specified event type.

    Args:
        patient_info (patientinfo.Patient): The patient information.
        event_type (str): The event type for the HL7 message.

    Returns:
        hl7apy.core.Message: The HL7 ADT message.
    """
    event_type = event_type.upper()  # Convert the event type to upper case

    try:
        # Construction is halted if return values is None 
        hl7 = create_message_header("ADT^" + event_type)
        hl7 = create_evn.create_evn(hl7, event_type=event_type) if hl7 else None
        hl7 = create_pid.create_pid(patient_info, hl7) if hl7 else None
        if event_type == "A01":
            hl7 = create_pv1.create_pv1(hl7) if hl7 else None
            
        if not hl7:
            logger.log(f"Message construction failed for patient within internal patient" \
                f" number {patient_info.internal_patient_number}", level="CRITICAL")
        return hl7
    except Exception as e:
        logger.log(f"Error creating ADT message (internal patient number {patient_info.internal_patient_number}): {e}", "CRITICAL")
        return None


def save_hl7_message_to_file(hl7_message:hl7apy.core.Message, hl7_folder_path:str):
    """Saves an HL7 message to a file.

    Args:
        hl7_message (hl7apy.core.Message): The HL7 message to save.
        hl7_folder_path (str): The folder path to save the HL7 message.
    """
    
    # Extract the year of birth
    try:
        dob_field = hl7_message.pid.pid_7.to_er7()
        # Check if DOB field is empty, too short, or contains invalid characters
        if not dob_field or len(dob_field.strip()) < 4 or not dob_field[:4].isdigit():
            year_of_birth = "unknown"
            logger.log(f"Invalid or empty date of birth field: '{dob_field}', using 'unknown' folder", "WARNING")
        else:
            year_of_birth = dob_field[0:4]
    except Exception as e:
        year_of_birth = "unknown"
        logger.log(f"Could not extract year of birth: {e}", "WARNING")
    
    # Use the shared counter in a thread-safe way
    with sequence_counter.get_lock():
        sequence_counter.value += 1
        current_sequence = sequence_counter.value
    
    # Create base folder if it doesn't exist
    hl7_folder_path = Path(hl7_folder_path)
    hl7_folder_path.mkdir(parents=True, exist_ok=True)
    
    # Create year of birth subdirectory
    year_folder_path = hl7_folder_path / year_of_birth
    year_folder_path.mkdir(parents=True, exist_ok=True)
    
    hl7_file_path = year_folder_path / f"{datetime.now().strftime('%Y%m%d%H%M%S')}.{current_sequence:08d}.hl7"
    
    # Add retry logic for handling temporary resource unavailability
    max_retries = 5
    retry_delay = 0.5  # Start with 0.5 seconds delay
    
    for attempt in range(max_retries):
        try:
            with open(hl7_file_path, "w+", newline="") as hl7_file:
                for child in hl7_message.children:
                    hl7_file.write(child.to_er7() + "\r")
                
                hl7_file.flush()  # Ensure the content is written to disk.
                
                # Move the file pointer to the beginning of the file.
                hl7_file.seek(0)

                # Read the content from the file.
                content = hl7_file.read()

                # Replace any CRLF pairs with a single CR (carriage return), and then replace any remaining LF (line feed) with CR.
                # This ensures that only CR (ASCII 13) is present.
                new_content = content.replace('\r\n', '\r').replace('\n', '\r')

                # Move back to the beginning of the file and truncate it.
                hl7_file.seek(0)
                hl7_file.truncate()

                # Write the normalized content back to the file.
                hl7_file.write(new_content)
                hl7_file.flush()  # Optionally flush changes again.
            
            # If we reach here, the file operation was successful
            logger.log(f"Successfully saved HL7 message to {hl7_file_path}", "INFO")
            return
            
        except BlockingIOError as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                logger.log(f"Attempt {attempt+1}/{max_retries}: Resource temporarily unavailable for {hl7_file_path}. Retrying in {retry_delay} seconds...", "WARNING")
                time.sleep(retry_delay)
                # Exponential backoff - increase delay for next attempt
                retry_delay *= 2
            else:
                logger.log(f"Failed to save HL7 message after {max_retries} attempts: {e}", "ERROR")
                raise  # Re-raise the exception after all retries fail
        except Exception as e:
            logger.log(f"Error saving HL7 message: {e}", "ERROR")
            raise


def save_hl7_messages_batch(hl7_messages, hl7_folder_path, batch_id):
    """Saves multiple HL7 messages as separate files.

    Args:
        hl7_messages (list): List of HL7 messages to save
        hl7_folder_path (str): The folder path to save the HL7 messages
        batch_id (int or str): Batch identifier (used for logging)
    """
    if not hl7_messages:
        return
        
    hl7_folder_path = Path(hl7_folder_path)
    # Create the base directory
    hl7_folder_path.mkdir(parents=True, exist_ok=True)
    
    # Add retry logic for handling temporary resource unavailability
    max_retries = 5
    retry_delay = 0.5  # Start with 0.5 seconds delay
    
    # Log the batch processing start with batch ID for better tracking
    logger.log(f"Starting to save {len(hl7_messages)} messages from batch {batch_id}", "INFO")
    
    for message in hl7_messages:
        # Extract the year of birth
        try:
            dob_field = message.pid.pid_7.to_er7()
            # Check if DOB field is empty, too short, or contains invalid characters
            if not dob_field or len(dob_field.strip()) < 4 or not dob_field[:4].isdigit():
                year_of_birth = "unknown"
                logger.log(f"Invalid or empty date of birth field: '{dob_field}', using 'unknown' folder", "WARNING")
            else:
                year_of_birth = dob_field[0:4]
        except Exception as e:
            year_of_birth = "unknown"
            logger.log(f"Could not extract year of birth: {e}", "WARNING")
        
        # Create year of birth subdirectory
        year_folder_path = hl7_folder_path / year_of_birth
        year_folder_path.mkdir(parents=True, exist_ok=True)
            
        # Use the shared counter in a thread-safe way for each message
        with sequence_counter.get_lock():
            sequence_counter.value += 1
            current_sequence = sequence_counter.value
            
        # Format: <YYYYMMDDHHMMSS>.<sequenceNum (8 digits)>.hl7
        hl7_file_path = year_folder_path / f"{datetime.now().strftime('%Y%m%d%H%M%S')}.{current_sequence:08d}.hl7"
        
        for attempt in range(max_retries):
            try:
                with open(hl7_file_path, "w", newline="") as hl7_file:
                    for child in message.children:
                        # Write directly with CR line endings
                        hl7_file.write(child.to_er7().replace('\r\n', '\r').replace('\n', '\r') + '\r')
                
                # If we reach here, the file operation was successful
                logger.log(f"Successfully saved HL7 message ({current_sequence}) from batch {batch_id} to {hl7_file_path}", "INFO")
                break
                
            except BlockingIOError as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    logger.log(f"HL7 message ({current_sequence}), Batch {batch_id}, Attempt {attempt+1}/{max_retries}: Resource temporarily unavailable for {hl7_file_path}. Retrying in {retry_delay} seconds...", "WARNING")
                    time.sleep(retry_delay)
                    # Exponential backoff - increase delay for next attempt
                    retry_delay *= 2
                else:
                    logger.log(f"HL7 message ({current_sequence}), Batch {batch_id}: Failed to save HL7 message after {max_retries} attempts: {e}", "ERROR")
                    raise  # Re-raise the exception after all retries fail
            except Exception as e:
                logger.log(f"HL7 message ({current_sequence}), Batch {batch_id}: Error saving HL7 message: {e}", "ERROR")
                raise
    
    logger.log(f"Completed saving all messages from batch {batch_id}", "INFO")
