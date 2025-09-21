# Test_Precision
This application is responsible for taking patient eligibility tracker items from main and transfering them into LIS Precision as test.

To be transferred, the Eligibility Trackers need to follow the following crieteria: 
    1 - The facility billing/shipping states need to match. For instance: IN, MD, OH, VA, WV
    2 - The Tracker shouldnt have been send to LIS 
    3- The tracker/patient wasnt already sent to the destination before 
        a- If a patient already has tracker, dont send them into salesforce even if its a new tracker. 
    4 - If there was a specimen already collected 
    5- The speciment collected date is AFTER september 15th 
    6- the specimen collected date is yesterday or earlier 

When transferred over to LIS Precision, the Patient, Doctor/ Provider and Account that is on the eligilibility tracker should also be on the test 
Therefore, if the Patient, Account or Provider isnt in salesforce, create them 
 -> Note: For accounts, there are parent accounts and child accounts, If an account on an eligibility tracker is a child account, make sure the parent account exists before adding the child account 

In addition, a test should also have the pdf documents that are on the eligibility tests. The pds should only be transferred if the title of the document has consent or requisition on it and it shouldnt have acutis or aegis. 

