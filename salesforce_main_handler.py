from salesforce_connector import *
import os 
import requests
from dotenv import load_dotenv
import time
from utils import *
load_dotenv()

'''
    This class is responsible for querying from geneid main
'''
class salesforce_main_handler: 
    
    def __init__(self):

        sf_url = os.environ.get('SF_MAIN_TOKEN_URL')
        sf_client_id = os.environ.get('SF_MAIN_CLIENT_ID')
        sf_client_secret = os.environ.get('SF_MAIN_CLIENT_SECRET')
        self.sf_client = get_s3_client()
        self.sf = get_sf_instance(sf_url,sf_client_id,sf_client_secret)
        pass




    def get_eligiblity_trackers(self, eligible_states): 
        soql = f"""SELECT Id, Name, Speciment_Collected_Date__c, 
        Patient__r.Name, Patient__r.First_Name__c, Patient__r.Last_Name__c, Patient__r.SSN__c, Patient__r.Id, Patient__r.DOB__c, Patient__r.Gender__c, 
            Patient__r.Normalized_SSN__c, Patient__r.Medicaid__c, Patient__r.Medicare__c, Patient__r.MRN__c,  Patient__r.Status__c,
        Account__r.Type, Account__r.Name,  Account__r.Id, Account__r.Website, Account__r.Description, Account__r.Phone, Account__r.Industry, Account__r.NumberOfEmployees,
        Account__r.Non_Aegis_Company__c, Account__r.Is_Parent_Account__c, Account__r.ParentId, 
        Doctor__r.First_Name__c, Doctor__r.Last_Name__c, Doctor__r.NPI__c, Doctor__r.Id, Doctor__r.Address__c, Doctor__r.Name, Doctor__r.Middle_Name__c
        FROM Eligiblity_Tracker__c 
        WHERE (Account__r.BillingState in ({eligible_states}) or Account__r.ShippingState in ({eligible_states})) and Sent_To_LIS__c != null and Sent_To_Precision_LIS_DateTime__c = null and Speciment_Collected_Date__c != null AND Speciment_Collected_Date__c <= YESTERDAY and Speciment_Collected_Date__c >= 2025-09-15
        ORDER BY Speciment_Collected_Date__c DESC"""
        results = self.sf.query_all(soql)
        eligiblility_trackers_dict = {}
        patient_tracker_dict = {}
        test_dict = {}
        for result in results['records']: 
            patientssn = result['Patient__r']['Normalized_SSN__c']
            patient_tracker_dict[patientssn] = result
            # if patientId not in eligiblility_trackers_dict:
                # test_dict[result['Id']] = []
            
            # test_dict[result['Id']].append(result)
            eligiblility_trackers_dict[result['Id']] = result
        
        return eligiblility_trackers_dict, patient_tracker_dict
    
    def get_all_accounts(self): 
        soql = """
            SELECT Id, Name, Website, Description, Phone, Industry, Type, NumberOfEmployees, Non_Aegis_Company__c, Is_Parent_Account__c, ParentId
            FROM Account 
        """
        results = self.sf.query_all(soql)
        account_dict = {}
        for result in results['records']: 
            account_dict[result['Id']] = result

        return account_dict
    
    def chunked(self, iterable, size):
        """Yield successive size-sized chunks from iterable."""
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    def get_all_tracker_pdfs(self, tracker_ids, batch_size=250):
        results = {}

        for chunk in self.chunked(tracker_ids, batch_size):
            # Build the IN clause safely
            in_clause = ",".join([f"'{tid}'" for tid in chunk])

            query = f"""
                SELECT
                   Id, Name, Link__c, Eligiblity_Tracker__c 
                FROM Tracker_Doc__c
                WHERE Eligiblity_Tracker__c IN ({in_clause})
            """

            records = self.sf.query_all(query)['records']
            for record in records:
                pdf_title = record['Name'].lower()
                if ('consent' in pdf_title or 'requisition' in pdf_title) and not 'acutis' in pdf_title and not 'aegis' in pdf_title:
                    pass
                else:
                    print("Skipping non-consent/requisition document: ", pdf_title)
                    continue

                pdf_link = record['Link__c']
                presigned_url = sign_link(self.sf_client, os.getenv('S3_BUCKET_NAME'), pdf_link, minutes=60)

                # If the link requires Salesforce auth:
                # resp = self.sf.session.get(url, headers=self.sf.headers)
                resp = requests.get(presigned_url)
                if resp.status_code == 200:
                    pdf_data = resp.content  # raw bytes
                else:
                    print(f"Failed to fetch file: {resp.status_code} - {resp.text}")
                    pdf_data = None
                if record['Eligiblity_Tracker__c'] not in results:
                    results[record['Eligiblity_Tracker__c']] = []
                record['pdf_data'] = pdf_data
                results[record['Eligiblity_Tracker__c']].append(record)

            

        return results
    
    def update_eligibility_trackers(self, tracker_list): 
        if len(tracker_list) > 0: 
            results = self.sf.bulk.Eligiblity_Tracker__c.update(tracker_list)
            print(results)


