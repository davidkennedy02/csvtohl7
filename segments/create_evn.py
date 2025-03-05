from logger import AppLogger
from segments.segment_utilities import create_obr_time

logger = AppLogger()

def create_evn(hl7, event_type:str="A01"):
    '''Creates a OBR segment for the HL7 message - requires a patient_info object and the hl7 message
    Note: the obr time is random, and will likely need looking at before release!!
    '''
    try:
        request_date = create_obr_time()

        hl7.evn.evn_1 = event_type
        hl7.evn.evn_2 = request_date
        
    except Exception as e:
        print("An AssertionError occurred:", e)
        print(f"Could not create EVN Segment: {e}")
        logger.log(f"An error of type {type(e).__name__} occurred. Arguments:\n{e.args}\n\nCould not create EVN segment", "CRITICAL")
        return None
    else:
        return hl7