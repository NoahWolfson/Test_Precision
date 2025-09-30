from salesforce_main_handler import salesforce_main_handler
from sf_lis_handler import sf_lis_handler
from utils import *
import time
'''
    This class is responsible for creating tests in LIS based on the eligiblilty trackers 
    found in main GENEID SF
'''
class test_creator: 

    def __init__(self):
        self.sf_lis = sf_lis_handler()
        self.CHUNK_SIZE = 25
        self.sf_main = salesforce_main_handler()
        self.patient_insurances, self.lis_insurances, self.lis_account, self.lis_doctors, self.lis_patients, self.eligibilty_by_main_id, self.eligiblity_by_main_patient_id, self.patient_list, self.eligibility_by_patient_ssn = self.sf_lis.get_sf_data()
        self.main_accounts = self.sf_main.get_all_accounts()
        pass


    def run_test_maker(self):
        #first get the eligible states that can be tested in 
    
        configure = self.sf_lis.get_configure('Precision_Test_States__c, Patient_Create_Fields_For_Main__c, Patient_Update_Fields_For_Main__c, Provider_Update_Fields_For_Main__c, Provider_Create_Fields_Main__c, Account_Create_Fields_For_Main__c, Account_Update_Fields_For_Main__c')
        eligible_states_formatted = format_state_codes(configure['Precision_Test_States__c']) 
        self.eligibility_tracker, self.patient_tracker_dict = self.sf_main.get_eligiblity_trackers(eligible_states_formatted)
        #first its going to be chuncked out to speed up the process
        self.trackers_not_created = []
        tracker_ids = list(self.eligibility_tracker.keys())
        for chunk in chunk_list(tracker_ids, self.CHUNK_SIZE):
            self.account_id_list = []
            self.created_provider_set = set()
            self.created_patient_set = set()
            self.created_insurance_set = set()
            self.facility_parent_company_dict = {}
            self.facility_company_dict = {}
            self.insurances_toCreate = []
            self.insurances_to_update = []
            self.patient_insurances_to_create = []
            self.create_provider_list = []
            self.facilities_to_create = []
            self.update_trackers_list = []
            self.create_tests_list = []
            self.update_provider_list = []
            self.create_patient_list = []
            self.update_patient_list = []
            self.eligiblilty_pdfs = self.sf_main.get_all_tracker_pdfs(chunk)
            #first loop through all trackers to check if the accounts, patients and providers are all in salesforce
            for main_id in chunk:
                tracker = self.eligibility_tracker.get(main_id, None)
                curr_patient = tracker.get('Patient__r', None)
                if curr_patient == None: 
                    continue 
                self.crud_accounts(tracker)
                #this is for modifying patient records
                self.crud_patient(configure, curr_patient)
                self.crud_insurances(tracker)
                

                curr_provider = tracker.get('Doctor__r', None)
                if curr_provider == None: 
                    continue
                self.crud_provider(configure, curr_provider)
            #now go through the parent companies and add them before adding the regular accounts
            for fac_id, details in self.facility_parent_company_dict.items(): 
                if details['parent_company'] == None and fac_id not in self.lis_account:
                    parent_company_id = None
                    account_info = details['info']
                    self.create_account(parent_company_id, account_info)
                    

            self.get_all_newly_created_accounts()
            #reset to create child accounts 
            self.facilities_to_create = []
            #once the parent accounts were created, add the child accounts 
            for fac_id, details in self.facility_company_dict.items(): 
                if details['parent_company'] != None and fac_id not in self.lis_account:
                    parent_company_id = self.lis_account.get(details['parent_company'], None).get('Id', None)
                    account_info = details['info']
                    self.create_account(parent_company_id, account_info)

            self.get_all_newly_created_accounts()

            self.sf_lis.update_providers(self.update_provider_list)
            new_providers = self.sf_lis.add_providers(self.create_provider_list)
            self.lis_doctors.update(new_providers)

            self.lis_insurances.update(self.insurances_to_update)
            new_insurances = self.sf_lis.create_insurances(self.insurances_toCreate)
            self.lis_insurances.update(new_insurances)

            self.sf_lis.update_patients(self.update_patient_list)
            new_patients, new_patient_list = self.sf_lis.add_patients(self.create_patient_list)
            self.lis_patients.update(new_patients)
            self.patient_list = new_patient_list

            #check if there are any patient insurances to create by looping through the patients 
            for patient in chunk:
                patient_on_tracker = self.eligibility_tracker.get(patient, None).get('Patient__r', None)
                insurance_on_tracker = self.eligibility_tracker.get(patient, None).get('Payer__r', None)
                patient_id = self.lis_patients.get(patient_on_tracker.get('Id', None)).get('Id', None)
                insurance_id = self.lis_insurances.get(insurance_on_tracker.get('Id', None)).get('Id', None)
                lis_patient_insurance = self.patient_insurances.get(patient_id) 
                if lis_patient_insurance is None: 
                    self.patient_insurances_to_create.append({
                        'Patient__c': patient_id,
                        'Insurance__c': insurance_id,
                        'Type__c': 'Primary',
                        'Name': f"{patient_on_tracker.get('Name', None)} - {insurance_on_tracker.get('Name', None)}"
                    })
                #if there already is a patient insurance, check if its the same insurance as on the tracker, if not update it
                elif lis_patient_insurance is not None and lis_patient_insurance.get('Insurance__c', None) != insurance_id:
                    self.patient_insurances_to_create.append({
                        'Patient__c': patient_id,
                        'Insurance__c': insurance_id, 
                        'Type__c': 'Primary',
                        'Name': f"{patient_on_tracker.get('Name', None)} - {insurance_on_tracker.get('Name', None)}"
                    })
            self.sf_lis.crrate_patient_insurances(self.patient_insurances_to_create)

            for tracker_id in chunk: 
                tracker = self.eligibility_tracker.get(tracker_id, None)
                patient_on_tracker = tracker.get('Patient__r', None)
                account_on_tracker = tracker.get('Account__r', None)
                provider_on_tracker = tracker.get('Doctor__r', None)
                lis_account = self.lis_account.get(account_on_tracker.get('Id', None), None)
                lis_patient = self.lis_patients.get(patient_on_tracker.get('Id', None), None)
                lis_provider = self.lis_doctors.get(provider_on_tracker.get('Id', None), None)
                tracker_files = self.eligiblilty_pdfs.get(tracker_id, [])
                #this means the tracker already exists within salesforce
                if tracker['Id'] in self.eligibilty_by_main_id: 
                    continue
                #this means that the tracker isnt the same inside the 
                elif patient_on_tracker['Id'] in self.eligiblity_by_main_patient_id or patient_on_tracker['Normalized_SSN__c'] in self.eligibility_by_patient_ssn: 
                    continue
                #this means that the tracker doesnt exist yet in salesforce, so add it as a test
                else:
                    self.create_tests_list.append({
                        'Name': tracker['Name'],
                        'Collection_DateTime__c': f"{tracker['Speciment_Collected_Date__c']}T00:00:00Z",
                        'Main_Salesforce_Tracker_Id__c' : tracker['Id'],
                        'Patient__c': lis_patient.get('Id', None) if lis_patient != None else None,
                        'Account__c': lis_account.get('Id', None) if lis_account != None else None,
                        'Referring_Doctor__c': lis_provider.get('Id', None) if lis_provider != None else None,
                        'LIS_Test_Type__c': 'PGX',
                        'Specimen_Type__c': 'Saliva', 
                        'Status__c': 'Pending',
                        'Panel_Type__c': 'Comprehensive'
                    })
            eligibilty_by_main_id, eligiblity_by_main_patient_id, eligibility_by_patient_ssn = self.sf_lis.create_tests(self.create_tests_list)
            self.eligibilty_by_main_id.update(eligibilty_by_main_id)
            self.eligiblity_by_main_patient_id.update(eligiblity_by_main_patient_id)
            self.eligibility_by_patient_ssn.update(eligibility_by_patient_ssn)

            for tracker_id in chunk:
                if tracker_id in self.eligibilty_by_main_id:
                    tracker_files = self.eligiblilty_pdfs.get(tracker_id, [])     
                    for tracker_file in tracker_files:
                        lis_tracker_id = self.eligibilty_by_main_id.get(tracker_id, None)
                        self.sf_lis.upload_pdf_to_salesforce(tracker_file['pdf_data'], lis_tracker_id['Id'], tracker_file['Name'])
                    self.update_trackers_list.append({
                        'Id': tracker_id,
                        'Sent_To_Precision_LIS_DateTime__c': time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    })
                else: 
                    self.trackers_not_created.append(self.eligibility_tracker.get(tracker_id))
            self.sf_main.update_eligibility_trackers(self.update_trackers_list)
            pass
        pass

    def crud_insurances(self, tracker):
        curr_payer = tracker.get('Payer__r', None)
        if curr_payer != None and curr_payer['Id'] not in self.lis_insurances and curr_payer['Id'] not in self.created_insurance_set: 
            self.insurances_toCreate.append({
                        'Name': curr_payer.get('Name', None),
                        'Insurance_ID__c': curr_payer.get('Insurance_ID__c', None),
                        'State__c': curr_payer.get('State__c', None),
                        'Type__c': curr_payer.get('Type__c', None),
                        'Payer_Code__c': curr_payer.get('Payer_Code__c', None),
                        'Insurance_Main_Id__c': curr_payer.get('Id', None)
                    })
            self.created_insurance_set.add(curr_payer['Id'])
        elif curr_payer['Id'] in self.created_insurance_set:
            pass
        else:
            temp_insurance_update_dict = {}
            curr_lis_insurance = self.lis_insurances.get(curr_payer['Id'], None)
            if curr_lis_insurance:
                if curr_lis_insurance['Name'] != curr_payer['Name']:
                    temp_insurance_update_dict['Name'] = curr_payer['Name']
                if curr_lis_insurance['Insurance_ID__c'] != curr_payer['Insurance_ID__c']:
                    temp_insurance_update_dict['Insurance_ID__c'] = curr_payer['Insurance_ID__c']
                if curr_lis_insurance['State__c'] != curr_payer['State__c']:
                    temp_insurance_update_dict['State__c'] = curr_payer['State__c']
                if curr_lis_insurance['Type__c'] != curr_payer['Type__c']:
                    temp_insurance_update_dict['Type__c'] = curr_payer['Type__c']
                if curr_lis_insurance['Payer_Code__c'] != curr_payer['Payer_Code__c']:
                    temp_insurance_update_dict['Payer_Code__c'] = curr_payer['Payer_Code__c']
                if curr_lis_insurance['Insurance_Main_Id__c'] != curr_payer['Id']:
                    temp_insurance_update_dict['Insurance_Main_Id__c'] = curr_payer['Id']
            if temp_insurance_update_dict:
                self.insurances_to_update.append({
                            'Id': curr_lis_insurance['Id'],
                            'fields': temp_insurance_update_dict
                        })


    def create_account(self, parent_company_id, account_info):
        self.facilities_to_create.append({
                    'Name': account_info['Name'],
                    'Phone': account_info.get('Phone', None),
                    'Website': account_info.get('Website', None),
                    'Description': account_info.get('Description', None),
                    'Industry': account_info.get('Industry', None),
                    'NumberOfEmployees': account_info.get('NumberOfEmployees', None),
                    'Type': account_info.get('Type', None),
                    'Non_Aegis_Company__c': account_info.get('Non_Aegis_Company__c', None),
                    'Is_a_Parent_Account__c': account_info.get('Is_Parent_Account__c', None),
                    'Main_Salesforce_Id__c': account_info.get('Id', None),
                    'ParentId': parent_company_id
                })

    def get_all_newly_created_accounts(self):
        all_accounts = self.sf_lis.add_accounts(self.facilities_to_create)
        if not all_accounts: 
            all_accounts = self.lis_account
        else: 
            self.lis_account = all_accounts

    def crud_provider(self, configure, curr_provider):        
        if curr_provider['Id'] in self.lis_doctors: 
            curr_lis_doctor = self.lis_doctors.get(curr_provider['Id'])
            temp_provider = {}
            update_provider_fields_str = configure.get('Provider_Update_Fields_Main__c')
            if update_provider_fields_str == None:
                return
            update_provider_fields = update_provider_fields_str.split(';')
            for update_field in update_provider_fields: 
                lis_update_field, main_update_field = update_field.split(':')
                if '.' in lis_update_field and '.' in main_update_field: 
                    lis_update_field_1, lis_update_field_2 = lis_update_field.split('.')
                    main_update_field_1, main_update_field_2 = main_update_field.split('.')
                    curr_main_value = curr_provider[main_update_field_1][main_update_field_2]
                    curr_lis_value = curr_lis_doctor[lis_update_field_1][lis_update_field_2]
                    if curr_main_value != curr_lis_value: 
                        if lis_update_field_1 not in temp_provider: 
                            temp_provider[lis_update_field_1] = {}
                        temp_provider[lis_update_field_1][lis_update_field_2] = curr_main_value
                else: 
                    curr_main_value = curr_provider[main_update_field]
                    curr_lis_value = curr_lis_doctor[lis_update_field]
                    if curr_main_value != curr_lis_value: 
                        temp_provider[lis_update_field] = curr_main_value
                if temp_provider: 
                    temp_provider['Id'] = curr_lis_doctor['Id']
                    self.update_provider_list.append(temp_provider)
        elif curr_provider['Id'] in self.created_provider_set: 
            pass         
        else: 
            self.created_provider_set.add(curr_provider['Id'])
            temp_provider = {}
            create_provider_fields_str = configure.get('Provider_Create_Fields_Main__c')
            create_provider_fields = create_provider_fields_str.split(';')
            for update_field in create_provider_fields: 
                lis_update_field, main_update_field = update_field.split(':')
                if '.' in lis_update_field and '.' in main_update_field: 
                    lis_create_field_1, lis_create_field_2 = lis_update_field.split('.')
                    main_update_field_1, main_update_field_2 = main_update_field.split('.')
                    curr_main_value = curr_provider[main_update_field_1][main_update_field_2]
                    if lis_create_field_1 not in temp_provider: 
                        temp_provider[lis_create_field_1] = {}
                    temp_provider[lis_create_field_1][lis_create_field_2] = curr_main_value
                else:
                    temp_provider[lis_update_field] = curr_provider[main_update_field]
            if temp_provider: 
                self.create_provider_list.append(temp_provider)

    def crud_patient(self, configure, curr_patient):
        found_patient = False
        lis_patient = None
        
        if curr_patient['Id'] in self.lis_patients: 
            lis_patient = self.lis_patients.get(curr_patient['Id'])
            self.update_patient(configure, lis_patient, curr_patient)
            found_patient = True
        if not found_patient:
            lis_patient = self.find_patient(curr_patient)
        if lis_patient: 
            self.update_patient(configure, lis_patient, curr_patient)
        elif curr_patient['Id']  in self.created_patient_set:
            pass
        else:
            self.create_patient(configure, curr_patient)
            self.created_patient_set.add(curr_patient['Id'])

    def create_patient(self, configure, curr_main_patient):
        patient_create_fields_str = configure['Patient_Create_Fields_For_Main__c']
        patient_create_fields = patient_create_fields_str.split(';')
        templ_create_dict = {}
        for patient_field in patient_create_fields:
            lis_patient_field, main_patient_field = patient_field.split(':')
            templ_create_dict[lis_patient_field] = curr_main_patient[main_patient_field]
        if templ_create_dict: 
            self.create_patient_list.append(templ_create_dict)

    def update_patient(self, configure, curr_lis_patient, curr_main_patient):
        patient_update_fields_str = configure['Patient_Update_Fields_For_Main__c']
        patient_update_fields = patient_update_fields_str.split(';')
        templ_update_dict = {}
        for patient_field in patient_update_fields:
            lis_patient_field, main_patient_field = patient_field.split(':')
            if curr_lis_patient[lis_patient_field] != curr_main_patient[main_patient_field]:
                templ_update_dict[lis_patient_field] = curr_main_patient[main_patient_field]
        if templ_update_dict: 
            templ_update_dict['Id'] = curr_lis_patient['Id']
            self.update_patient_list.append(templ_update_dict)

    def find_patient(self, curr_patient):   
        found_patient_record = None
        for lis_patient in self.patient_list:
            lis_patient_f_name = lis_patient['First_Name__c']
            lis_patient_l_name = lis_patient['Last_Name__c']
            list_patient_ssn = lis_patient['Normalized_SSN__c']
            main_patient_first_name = curr_patient['First_Name__c']
            main_patient_last_name = curr_patient['Last_Name__c']
            main_patient_ssn = curr_patient['Normalized_SSN__c']
            if lis_patient_f_name == main_patient_first_name and lis_patient_l_name == main_patient_last_name and list_patient_ssn == main_patient_ssn: 
                found_patient_record = lis_patient
                break

        return found_patient_record 

    def crud_accounts(self, tracker):
        facility = None
        parent_company = None
        if tracker.get('Account__r', None) != None:
            facility = tracker['Account__r']['Id']
            if tracker['Account__r']['ParentId'] != None:
                parent_company = tracker['Account__r']['ParentId']
        if parent_company not in self.lis_account:
            self.account_id_list.append(parent_company)
            parent_company_info = self.main_accounts.get(parent_company)
            self.facility_parent_company_dict[parent_company] = {'info':parent_company_info,'parent_company':None} 
        if facility not in self.lis_account:
            self.account_id_list.append(facility)
            self.facility_company_dict[facility] = {'info':tracker['Account__r'], 'parent_company':parent_company}
        
                



if __name__ == "__main__":
    testCreator = test_creator()
    testCreator.run_test_maker()
    pass
