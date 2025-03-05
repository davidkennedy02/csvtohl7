import csv
from datetime import date, datetime
import os
import hl7_utilities
import patientinfo
from logger import AppLogger

CSV_INPUT_FOLDER = "input_csv"
HL7_OUTPUT_FOLDER = "output_hl7"

logger = AppLogger()


def calculate_age(birth_date):
    """Calculate the current age from a date of birth.

    Args:
        birth_date (datetime | str): The date of birth used to calculate age.

    Returns:
        int: The current age derived from the date of birth.
    """
    today = date.today()
    if type(birth_date) == str:
        birth_date = datetime.strptime(birth_date, "%Y%m%d")
    age = (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )
    return age


def process_csv_folder():
    """Processes all CSV files in a specified folder.

    This function looks for CSV files in the `CSV_INPUT_FOLDER`, reads each file,
    extracts patient information, and creates HL7 messages for each patient.
    The HL7 messages are then saved to the `HL7_OUTPUT_FOLDER`.
    """
    # Look in the CSV_INPUT_FOLDER to check for CSV files.
    for filename in os.listdir(CSV_INPUT_FOLDER):
        if filename.lower().endswith(".csv"):
            input_file = os.path.join(CSV_INPUT_FOLDER, filename)
            
            # For each CSV file found...
            with open(input_file, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Extract information from each row in the CSV file, and use it to construct a `Patient` object. 
                for row in reader:
                    patient_info = patientinfo.Patient(**row)
                    
                    # Minor exclusion checks
                    if not patient_info.surname:
                        logger.log("Skipping patient - missing required surname", "WARNING")
                    elif patient_info.date_of_birth and (patient_info.date_of_death is None) \
                                                and (calculate_age(patient_info.date_of_birth) > 112):
                                                    logger.log("Skipping patient - no DOD, and age > 112", "INFO")
                    elif patient_info.death_indicator and patient_info.date_of_death \
                                                    and (patient_info.death_indicator == 'Y') \
                                                    and (calculate_age(patient_info.date_of_death) > 2):
                                                        logger.log("Skipping patient - dod > 2 years ago", "INFO")
                    else:
                        # Create the A28 HL7 message, and save within the `HL7_OUTPUT_FOLDER`
                        hl7_message = hl7_utilities.create_adt_message(patient_info=patient_info, event_type="A28")
                        hl7_utilities.save_hl7_message_to_file(hl7_message=hl7_message, hl7_folder_path=HL7_OUTPUT_FOLDER)
                

if __name__ == "__main__":
    process_csv_folder()
