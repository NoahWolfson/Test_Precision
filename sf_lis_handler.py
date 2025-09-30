from salesforce_connector import *
import os 
from dotenv import load_dotenv
import time
import base64


load_dotenv()

'''
    This class is responsible for querying from geneid main
'''
class sf_lis_handler: 
    
    def __init__(self):

        sf_url = os.environ.get('PRECISION_TOKEN_URL')
        sf_client_id = os.environ.get('PRECISION_CLIENT_ID')
        sf_client_secret = os.environ.get('PRECISION_CLIENT_SECRET')

        self.sf = get_sf_instance(sf_url,sf_client_id,sf_client_secret)
        pass


    def get_configure(self, configure_fields: str): 
        soql = f"SELECT {configure_fields} FROM Configure__c WHERE Id='a0Ca5000073Dib6EAC'" 
        results = self.sf.query(soql)
        return results['records'][0] 
    
    def get_sf_data(self): 
        eligibilty_by_main_id, eligiblity_by_main_patient_id, eligibility_by_patient_ssn = self.get_eligiblity_trackers()
        patient_dict, patient_list = self.get_patients()
        return self.get_patient_insurances(), self.get_Insurances(), self.get_accounts(), self.get_doctors(), patient_dict, eligibilty_by_main_id, eligiblity_by_main_patient_id, patient_list, eligibility_by_patient_ssn
    
    def get_patient_insurances(self): 
        soql = """SELECT Id, Name, Patient__c, Insurance__c, Type__c 
                FROM Patient_Insurance__c"""
        results = self.sf.query_all(soql)
        insurance_dict = {}
        for result in results['records']: 
            insurance_dict[result['Patient__c']] = result
        return insurance_dict

    def get_accounts(self): 
        soql = f"""SELECT Type, Name, Id, Website, Description, Phone,
            Industry, NumberOfEmployees,
            Non_Aegis_Company__c, Is_a_Parent_Account__c, ParentId, Main_Salesforce_Id__c
            FROM Account"""
        results = self.sf.query_all(soql)
        account_dict = {}
        for result in results['records']: 
            account_dict[result['Main_Salesforce_Id__c']] = result
        return account_dict
    
    def get_patients(self): 
        soql = f"""
            SELECT Id, Normalized_SSN__c, Name, Date_of_Birth__c, Last_Name__c, First_Name__c, Gender__c, GID_Salesforce_Id__c, Main_Salesforce_Id__c, 
            Medicaid__c, Medicare__c, MRN__c, SSN__c, Status__c, Searchable_DOB__c, Added_to_EMR__c, Count_of_Test_Results__c
            FROM Patient__c
        """
        resuls = self.sf.query_all(soql)
        patient_dict = {}
        patient_list = []
        for result in resuls['records']: 
            patient_dict[result['Main_Salesforce_Id__c']] = result
            patient_list.append(result)
        return patient_dict, patient_list
    
    def get_doctors(self):
        soql = """SELECT Id, First_Name__c, Last_Name__c, NPI__c, Main_Salesforce_Id__c, Address__c 
                FROM Doctor__c"""
        resuls = self.sf.query_all(soql)
        doctor_dict = {}
        for result in resuls['records']: 
            doctor_dict[result['Main_Salesforce_Id__c']] = result
        return doctor_dict
    
    def get_Insurances(self): 
        soql = """SELECT Id, Name, Insurance_ID__c, State__c, Type__c, Payer_Code__c, Insurance_Main_Id__c
                FROM Insurance__c"""
        resuls = self.sf.query_all(soql)
        insurance_dict = {}
        for result in resuls['records']: 
            insurance_dict[result['Insurance_Main_Id__c']] = result
        return insurance_dict
         
    def get_eligiblity_trackers(self): 
        soql = """SELECT Id, Name, Collection_DateTime__c, Main_Salesforce_Tracker_Id__c, Tracker_ID__c,
        Patient__r.Name, Patient__r.Main_Salesforce_Id__c, Patient__r.First_Name__c, Patient__r.Last_Name__c, Patient__r.SSN__c, Patient__r.Id, Patient__r.Date_of_Birth__c, Patient__r.Gender__c, Patient__r.Normalized_SSN__c,
            Patient__r.Medicaid__c, Patient__r.Medicare__c, Patient__r.MRN__c,  Patient__r.Status__c,
        Account__r.Main_Salesforce_Id__c, Account__r.Type, Account__r.Name,  Account__r.Id, Account__r.Website, Account__r.Description, Account__r.Phone, Account__r.Industry, Account__r.NumberOfEmployees,
        Account__r.Non_Aegis_Company__c, Account__r.Is_a_Parent_Account__c, Account__r.ParentId, 
        Referring_Doctor__r.Main_Salesforce_Id__c, Referring_Doctor__r.First_Name__c, Referring_Doctor__r.Last_Name__c, Referring_Doctor__r.NPI__c, Referring_Doctor__r.Id, Referring_Doctor__r.Address__c, Referring_Doctor__r.Name
        FROM Test_Result__c """
        results = self.sf.query_all(soql)
        eligibilty_by_main_id = {}
        eligiblity_by_main_patient_id = {}
        eligibility_by_patient_ssn = {}
        for result in results['records']: 
            eligibilty_by_main_id[result['Main_Salesforce_Tracker_Id__c']] = result
            eligiblity_by_main_patient_id[result['Patient__r']['Main_Salesforce_Id__c']] = result
            eligibility_by_patient_ssn[result['Patient__r']['Normalized_SSN__c']] = result
        return eligibilty_by_main_id, eligiblity_by_main_patient_id, eligibility_by_patient_ssn

        
    def add_accounts(self, account_list): 
        account_id_list = []
        if len(account_list) > 0: 
            results = self.sf.bulk.Account.insert(account_list)
            print(results)
            return self.get_accounts()
        else: 
            return {}
    def add_providers(self, provider_list): 
        if len(provider_list) > 0: 
            results = self.sf.bulk.Doctor__c.insert(provider_list)
            print(results)
            return self.get_doctors()
        else: 
            return {}
        
    def add_patients(self, patient_list): 
        if len(patient_list) > 0: 
            results = self.sf.bulk.Patient__c.insert(patient_list)
            print(results)
            return self.get_patients()
        else: 
            return {}, []
    def create_tests(self, test_list): 
        if len(test_list) > 0: 
            results = self.sf.bulk.Test_Result__c.insert(test_list)
            print(results)
            return self.get_eligiblity_trackers()
        else: 
            return {}, {}
        
    def create_insurances(self, insurance_list): 
        if len(insurance_list) > 0: 
            results = self.sf.bulk.Insurance__c.insert(insurance_list)
            print(results)
            return self.get_Insurances()
        else: 
            return {}
        
    def crrate_patient_insurances(self, patient_insurance_list):
        if len(patient_insurance_list) > 0: 
            results = self.sf.bulk.Patient_Insurance__c.insert(patient_insurance_list)
            print(results)
            return self.get_patient_insurances()
        else: 
            return {}
        
    def update_insurances(self, insurance_list): 
        if len(insurance_list) > 0: 
            results = self.sf.bulk.Insurance__c.update(insurance_list)
            print(results)
            

    def update_patients(self, patient_list): 
        if len(patient_list) > 0: 
            results = self.sf.bulk.Patient__c.update(patient_list)
            print(results)

    def update_providers(self, provider_list): 
        if len(provider_list) > 0: 
            results = self.sf.bulk.Doctor__c.update(provider_list)
            print(results)  

    def upload_pdf_to_salesforce(self, pdf_file_data, linked_entity_id, title):
        """
        Method to attach the given pdf to given patient id into both salesforce using content version document object.
        """
        name, ext = os.path.splitext(title)

        content_version = {
            "PathOnClient": title,
            "Title": name,
            "VersionData": base64.b64encode(pdf_file_data).decode("utf-8"),
        }
        content_version_response = self.sf.ContentVersion.create(content_version)
        content_version_id = content_version_response.get("id")

        if not content_version_id:
            return None

        content_version_details = self.sf.ContentVersion.get(content_version_id)
        content_document_id = content_version_details.get("ContentDocumentId")

        if content_document_id:
            link_data = {
                "ContentDocumentId": content_document_id,
                "LinkedEntityId": linked_entity_id,
                "ShareType": "V",
            }
            link_response = self.sf.ContentDocumentLink.create(link_data)
            return content_document_id

        return None
            
