# CSV to HL7 Processing Script

## Overview
This script is designed to process CSV files located within the `input_csv` folder. If one or more CSV files are found, the script reads each file, extracts information contained therein, and creates a `Patient` object. The `Patient` class definition can be found in `patientinfo.py`. An ADT^A01 HL7 message will then be constructed using the patient information, along with other details contained in files within the `segments` directory, each named according to the segment of the HL7 message they pertain to. Finally, the HL7 message will be saved as a flat file to the `output_hl7` directory.

Please make sure to review the [important notes section](#important-notes) I have left for whomever is reviewing this script.

## Project Structure 
The directory layout should looks as follows:
- CSVTOHL7 (root of directory)
    - input_csv
    - logs
    - output_hl7
    - segments
        - create_evn.py
        - create_msh.py
        - create_pid.py
        - create_pv1.py
        - segment_utilities.py
    - supplementary_docs
        - hl7_csv_guidance.csv
        - inferred_field_mappings.csv
    - test_cases
        - patient_test_cases.csv
        - ...
    - hl7_utilities.py
    - logger.py
    - main.py
    - patientinfo.py
    - README

If the layout of the project should be changed, ensure that the paths used by the script are modified as appropriate. 

## Data Validation
During the creation of the `Patient` object, validation checks ensure that the contents of each field comply with the maximum permitted lengths specified in `supplementary_docs/hl7_csv_guidance.csv`.

## HL7 Message Construction
Once the `Patient` object has been validated, an HL7 message is constructed using functions from `hl7_utilities.py` and segment generators located in the `segments` folder. If the message is successfully built, it is saved in the `output_hl7` folder.

## File Naming Convention
Currently, output files are named using a combination of the current date-time, the patient's surname, and the internal patient number. However, this naming convention should be improved to improve robustness and ease of understanding. 

## Script Execution
The main logic of the script is contained within `main.py`. This file also includes a small number of checks to enforce agreed-upon exclusions. To run the script, first ensure that the CSV files you wish to use to generate HL7 messages are found within the `input_csv` folder. Following this, simply execute the script with `python main.py`, or a similar command, from the root of the directory. The resulting HL7 messages should appear in the `output_hl7` folder.

## Logging
Basic logging is implemented throughout the execution of the script to satisfy requirements regarding notifying the Data Quality team; logs can be found in the `logs` folder. Logging should be improved to allow proper tracing of the source of errors, as currently the logs will only flag the problematic item, rather than an entire trace of, e.g. the CSV row which includes the problematic data, or a description of the patient. 

## Testing
Test cases for the script are stored in the `test_cases` folder. Further testing should be performed to ensure the script functions as necessary, along with the inclusion of robust safety nets and expansive exception-handling try-except-else blocks. 

## Important Notes
- The `MSH`, `EVN`, and `PV1` segments currently contain randomly generated data due to a lack of provided information. These fields should be modified as necessary.
- Due to a limited understanding of the client's requirements and the data they work with, some HL7 fields may contain misplaced data. I have provided a CSV file to illustrate the inferences made when attempting to map the supplied data to their appropriate HL7 fields - please review this document and make any required changes. The illustration can be found at `supplementary_docs/inferred_field_mappings.csv`. 

## Contact Information
For any questions regarding this script, please contact David Kennedy by email at [david.kennedy@cirdan.com](mailto:david.kennedy@cirdan.com), Monday and Wednesday from 9am to 5pm GMT.

