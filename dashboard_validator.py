import pandas as pd
import looker_sdk
import urllib3
import time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # Disabling https warning (self-signed warning), remove when accessing your own endpoint
print("Imports Loaded")

class Looker:
    def __init__(self) -> None:
        self.sdk = looker_sdk.init40()
        self.me = self.sdk.me()

    def switch_environment(self,environment:str) -> object:
        """
        overview: 
         - Switches either to production or development environment
        args:
         - environment:str -> Values are either: 'production' or 'dev'
        :returns: 
         - session object -> Use to confirm the session.workspace_id == desired_environment, i.e production or dev  
        """
        print(f"Switching to {environment}:")
        body = {"workspace_id":environment}
        self.sdk.update_session(body=body)
    
    def get_session(self) -> object:
        """
        :returns:
         - Looker_SDK Session Object 
        """
        return self.sdk.session()

    def get_branches_in_session(self):
        return None
        


    def __str__(self) -> str:
        return f"{self.me.__dict__}"

class Dashboard(Looker): 
    def __init__(self,dashboard_id) -> None:
        self.dashboard_id = dashboard_id

    def get_all_dashboard_elements(self) -> list: 
        return Looker.sdk.dashboard_dashboard_elements(self.dashboard_id)

    def sort_all_columns(self,df) -> pd.DataFrame:
        return None
    

if __name__ == '__main__':
    test_prod = Looker()
    print("Current session environment:")
    print(test_prod.get_session())

    print("Swap Environment")
    test_prod.switch_environment('dev')
    print(test_prod.get_session())
    time.sleep(1)
    
    print("Testing switching back to prod:")
    test_prod.switch_environment('production')
    print(test_prod.get_session())


