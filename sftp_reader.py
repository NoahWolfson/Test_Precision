import os
import paramiko
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from edi_835_parser import parse   # ✅ new import

load_dotenv()

class sftp_reader:
    def __init__(self):
        self.host = os.getenv('SFTP_HOST')
        self.port = int(os.getenv('SFTP_PORT', 22))
        self.username = os.getenv('SFTP_USER')
        self.password = os.getenv('SFTP_PASS')

    def read_895_files_from_sftp(self, remote_dir, local_dir='downloaded_files'):
        """
        Connects to an SFTP server and downloads .835 files modified in the last 30 days
        into local_dir, then processes them.
        """
        #self.delete_all_files_in_folder(local_dir)
        os.makedirs(local_dir, exist_ok=True)  # Ensure folder exists

        # --- Uncomment this if you want live SFTP downloads ---
        transport = paramiko.Transport((self.host, self.port))
        try:
            transport.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
        
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=30)
        
            for filename in sftp.listdir(remote_dir):
                if filename.endswith('.835'):
                    remote_path = f"{remote_dir}/{filename}"
                    file_attr = sftp.stat(remote_path)
                    file_mtime = datetime.fromtimestamp(file_attr.st_mtime, tz=timezone.utc)
                    if file_mtime >= cutoff:
                        local_path = os.path.join(local_dir, filename)
                        sftp.get(remote_path, local_path)  # Download file
        
            sftp.close()
        finally:
            transport.close()

        # Process whatever is in local_dir
        return self.process_downloaded_835_files(local_dir)
    
    import os

    def delete_all_files_in_folder(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

    def process_downloaded_835_files(self, local_dir='downloaded_files'):
        """
        Loops through all .835 files in the local_dir, parses them using edi-835-parser,
        and extracts claims into clean dicts.
        Returns a dictionary: { filename: [ {claim_id, patient_name, charge_amount, paid_amount, status, claim_type, icn} ] }
        """
        claims_by_claim_id = {}
        claims_by_medicare = {}
        claims_by_insurance = {}
        claims_list = []
        for filename in os.listdir(local_dir):
            if filename.endswith('.835'):
                file_path = os.path.join(local_dir, filename)
                transaction_sets = parse(file_path)   # TransactionSets object
                for ts in transaction_sets:
                    for claim in ts.claims:
                        # Patient info
                        patient = None
                        curr_claim = None 
                        if hasattr(claim, "patient") and claim.patient:
                            patient = claim.patient
                        if hasattr(claim, "claim") and claim.patient:
                            curr_claim = claim.claim
                        date = claim.dates
                        curr_icn = claim.claim.icn 
                        marker = claim.claim.marker
                        id_number = claim.patient.identification_code
                        # Claim info
                        temp_dict = {"Patient": patient,  # convert Status object to str
                            "Claim": curr_claim, 
                            "date": date}
                        claims_by_claim_id[marker] = temp_dict
                        claims_by_medicare[id_number] = temp_dict
                        claims_by_insurance[curr_icn] = temp_dict
                        claims_list.append(temp_dict)


        return claims_by_claim_id, claims_by_medicare, claims_by_insurance, claims_list
