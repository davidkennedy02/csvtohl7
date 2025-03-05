# CSV to HL7 Processing Script

## Overview
This script is designed to process CSV files located within the `input_csv` folder. If one or more CSV files are found, the script reads each file, extracts information contained therein, and creates a `Patient` object. The `Patient` class definition can be found in `patientinfo.py`. An ``ADT^A01`` or ``ADT^A28`` HL7 message will then be constructed using the patient information, along with other details contained in files within the `segments` directory, each named according to the segment of the HL7 message they pertain to. Finally, the HL7 message will be saved as a flat file to the `output_hl7` directory.

Please make sure to review the [important notes](#important-notes). 

If you have any questions or queries, please refer to the [contact information](#contact-information) below.

## Project Structure 
The directory layout should look as follows:
- CSVTOHL7 (root of directory)
    - input_csv
    - logs
    - output_hl7
    - sample_CSVs
        - ``patient_test_cases_edge.csv``
        - ``patient_test_cases_invalid.csv``
        - ``patient_test_cases_valid_extended.csv``
        - ``patient_test_cases_valid.csv``
    - segments
        - ``create_evn.py``
        - ``create_msh.py``
        - ``create_pid.py``
        - ``create_pv1.py``
        - ``segment_utilities.py``
    - tests
        - ``test_utilities.py``
    - ``hl7_utilities.py``
    - ``logger.py``
    - ``main.py``
    - ``patientinfo.py``
    - ``README.md``

If the layout of the project should be changed, ensure that the paths used by the script are modified as appropriate. 

## Data Validation
During the creation of the `Patient` object, validation checks ensure that the contents of each field comply with the maximum permitted lengths.

## HL7 Message Construction
Once the `Patient` object has been validated, an HL7 message is constructed using functions from `hl7_utilities.py` and segment generators located in the `segments` folder. If the message is successfully built, it is saved in the `output_hl7` folder.

## File Naming Convention
Currently, output files are named using a combination of the current date-time and a sequence number to ensure uniqueness.

## Script Execution
The main logic of the script is contained within `main.py`. This file also includes a small number of checks to enforce agreed-upon exclusions. To run the script, first ensure that the CSV files you wish to use to generate HL7 messages are found within the `input_csv` folder. Additionally, ensure that the required dependencies have been installed using ``python -m pip install -r requirements.txt``. Following this, simply execute the script with `python main.py`, or a similar command, from the root of the directory. The resulting HL7 messages should appear in the `output_hl7` folder.

## Logging
Basic logging is implemented throughout the execution of the script to satisfy requirements regarding notifying the Data Quality team; logs can be found in the `logs` folder. Logging will flag problematic CSV rows by referencing the patient internal number. 

## Testing
Test cases for the script are stored in the `tests` folder. Further testing should be performed to ensure the script functions as necessary, along with the inclusion of robust safety nets and expansive exception-handling try-except-else blocks. 

## Important Notes
- Some HL7 fields may contain misplaced data. Please review the code and make any required changes.

## Contact Information
For any questions regarding this script, please contact David Kennedy or Nicholas Campbell by email at [david.kennedy@cirdan.com](mailto:david.kennedy@cirdan.com) or [nicholas.campbell@cirdan.com](mailto:nicholas.campbell@cirdan.com) respectively, Monday and Wednesday from 9am to 5pm GMT.

