from salesforce_main_handler import salesforce_main_handler
import json
from sftp_reader import sftp_reader
class payer: 

    def __init__(self): 
        self.sf_handler = salesforce_main_handler()
        self.sftp_reader = sftp_reader()
        self.chunk_size = 200
        pass

    def run_payer(self): 
        self.patient_tracker_update_list = []
        self.patient_payment_dict = {}
        self.claims_by_claim_id, self.claims_by_medicare, self.claims_by_insurance, self.claims_list = self.sftp_reader.read_895_files_from_sftp('/responses')
        self.process_claims_in_chunks()
        #once all the claims are processed, check each patient claim and get the most recent claim based on the date
        for patient_id, payment_list in self.patient_payment_dict.items():
            most_recent_claim = None 
            for claim in payment_list:
                if len(payment_list) > 1: 
                    pass
                claim_date = claim['Date_Payment_Received__c']
                if most_recent_claim is None or claim_date > most_recent_claim['Date_Payment_Received__c']:
                    most_recent_claim = claim
            if most_recent_claim is not None: 
                self.patient_tracker_update_list.append({
                    'Id': most_recent_claim['Id'],
                    'Amount_Paid__c': most_recent_claim['Amount_Paid__c'],
                    'Payment_To_Precision_Amount__c': most_recent_claim['Payment_To_Precision_Amount__c']
                })

        self.sf_handler.update_eligibility_trackers(self.patient_tracker_update_list)

                
           


    def patient_checker(self, sf_patient_f_name, sf_patient_l_name, patient_record_name): 
        patient_record_last_name = patient_record_name['Patient'].last_name.strip().lower() if patient_record_name['Patient'].last_name else ''
        patient_record_first_name = patient_record_name['Patient'].first_name.strip().lower() if patient_record_name['Patient'].first_name else ''
        if sf_patient_f_name == patient_record_first_name and sf_patient_l_name == patient_record_last_name: 
            return True
        return False
    
    def process_claims_in_chunks(self):
        """
        Processes claims in chunks of 200.
        """
        for i in range(0, len(self.claims_list), self.chunk_size):
            chunk = self.claims_list[i:i + self.chunk_size]
            self.claim_id_dict = {}
            self.medicare_dict = {}
            self.insurance_dict = {}
            medicare_numbers = []
            insurance_numbers = []
            for claim in chunk:
                
                # Access attributes directly for tuples/objects
                medicare = claim['Patient'].identification_code
                insurance = claim['Claim'].icn
                claim_number = claim['Claim'].marker
                if medicare:
                    medicare_numbers.append(f"'{medicare}'")
                    self.medicare_dict[medicare] = claim
                if insurance:
                    insurance_numbers.append(f"'{insurance}'")
                    self.insurance_dict[insurance] = claim
                if claim_number:
                    self.claim_id_dict[claim_number] = claim
            formatted_medicare = ', '.join(medicare_numbers)
            formatted_insurance = ', '.join(insurance_numbers)
            sf_patient_dict = self.sf_handler.get_payer_eligible_trackers_for_payers(formatted_medicare, formatted_insurance)
            for patient_record in sf_patient_dict.values():
                temp_record = {}
                patient = patient_record['Patient__r']
                patient_first_name = patient['First_Name__c'].strip().lower() if patient['First_Name__c'] else ''
                patient_last_name = patient['Last_Name__c'].strip().lower() if patient['Last_Name__c'] else ''
                patient_medicare_number = patient['Medicare__c'].strip() if patient['Medicare__c'] else ''
                patient_insurance_number = patient['Insurance__c'].strip() if patient['Insurance__c'] else ''
                patient_medicaid_number = patient['Medicaid__c'].strip() if patient['Medicaid__c'] else ''
                patient_match = False
                curr_patient_record = self.medicare_dict.get(patient_medicare_number)
                if curr_patient_record is not None: 
                    patient_match = self.patient_checker(patient_first_name, patient_last_name, curr_patient_record)

                if not patient_match: 
                    curr_patient_record = self.insurance_dict.get(patient_insurance_number)
                    if curr_patient_record is not None: 
                        patient_match = self.patient_checker(patient_first_name, patient_last_name, curr_patient_record['Name'])

                if patient_match: 
                    claim = curr_patient_record['Claim']
                    if claim.paid_amount != 0: 
                        if patient_record['Id'] not in self.patient_payment_dict:
                            self.patient_payment_dict[patient_record['Id']] = []
                        #temp_record['Id'] = patient_record['Id']
                        #patient_record['Precision_Final_Payer__c'] = 'Medicare' if patient_medicare_number == curr_patient_record['Patient'].identification_code else 'Insurance'
                        patient_record['Amount_Paid__c'] = float(claim.paid_amount) if claim.paid_amount else 0.0
                        patient_record['Payment_To_Precision_Amount__c'] = float(claim.charge_amount) if claim.charge_amount else 0.0
                        patient_record['Date_Payment_Received__c'] = curr_patient_record['date'][0].date 
                        self.patient_payment_dict[patient_record['Id']].append(patient_record)
                

            pass

if __name__ == "__main__":
    payer_instance = payer()
    payer_instance.run_payer()