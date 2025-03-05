from logger import AppLogger
import traceback

logger = AppLogger()

# Creates a PV1 segment for the HL7 message requires a patient_info object and the hl7 message
def create_pv1(hl7):
    try:
        hl7.pv1.pv1_1 = "1"  # Set Patient Class to Inpatient
        hl7.pv1.pv1_2 = "O"  # Set Visit Number
        hl7.pv1.pv1_3 = "Visit Institution"  # Set Visit Institution
        hl7.pv1.pv1_7 = "^ACON"  # Set Patient Class to Inpatient
        hl7.pv1.pv1_8 = "^ANAESTHETICS CONS^^^^^^L"  # Set Patient Type to Ambulatory
        hl7.pv1.pv1_9 = "^ANAESTHETICS CONS^^^^^^^AUSHICPR"
    except Exception as e:
        print("An AssertionError occurred:", e)
        print(f"Could not create MSH Segment: {e}")
        logger.log(
            f"An error of type {type(e).__name__} occurred. Arguments:\n{e.args}\n\nCould not create MSH segment", "CRITICAL")
        return None
    else:
        return hl7