from datetime import date, datetime
import os
from pathlib import Path
from hl7apy import core
import hl7apy.core
from logger import AppLogger
from segments import create_pid, create_msh, create_evn, create_pv1
import patientinfo

sequenceNum = 0
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
        logger.log(f"Error creating ADT message: {e}", "CRITICAL")
        return None


def save_hl7_message_to_file(hl7_message:hl7apy.core.Message, hl7_folder_path:str):
    """Saves an HL7 message to a file.

    Args:
        hl7_message (hl7apy.core.Message): The HL7 message to save.
        patient_info (patientinfo.Patient): The patient information.
        hl7_folder_path (str): The folder path to save the HL7 message.
    """
    
    global sequenceNum
    sequenceNum += 1
    
    hl7_folder_path = Path(hl7_folder_path)
    hl7_file_path = hl7_folder_path / f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.{sequenceNum}.hl7"
    os.makedirs(os.path.dirname(hl7_file_path), exist_ok=True)
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
