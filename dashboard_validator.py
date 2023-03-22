import pandas as pd
import looker_sdk
import urllib3
import time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # Disabling https warning (self-signed warning), remove when accessing your own endpoint

class LookerEnvironment:
    def __init__(self,environment:str) -> None:
        self.sdk = looker_sdk.init40()
        self.me = self.sdk.me()
        self.environment = environment

    def switch_environment(self) -> None:
        """
        overview: 
         - Switches either to production or development environment
        args:
         - environment:str -> Values are either: 'production' or 'dev'
        :returns: 
         - session object -> Use to confirm the session.workspace_id == desired_environment, i.e production or dev  
        """
        print(f"Switching to {self.environment}:")
        body = {"workspace_id":self.environment}
        self.sdk.update_session(body=body)
    
    def checkout_dev_branch(self,project_name:str,branch_name:str) -> None:
        body = {"name":branch_name}
        self.sdk.update_git_branch(project_id=project_name, body=body)
        print(f"Switched to {branch_name} in {project_name}")
    
    def get_session(self) -> object:
        """
        :returns:
         - Looker_SDK Session Object
         - Example: ApiSession(can={'view': True, 'update': True}, workspace_id='production', sudo_user_id=None)
        """
        return self.sdk.session()

    def __str__(self) -> str:
        return f"{self.me.__dict__}"

class Dashboard: 
    def __init__(self,dashboard_id) -> None:
        self.dashboard_id = dashboard_id

    def get_all_dashboard_elements(self, sdk:object) -> list: 
        return sdk.dashboard_dashboard_elements(self.dashboard_id)

    def sort_all_columns(self,df) -> pd.DataFrame:
        return df.sort_values(by=df.columns.tolist())
    
    def get_all_tiles_data(self,sdk:object) -> list:
        tiles_in_dashboard = self.get_all_dashboard_elements(sdk)
        dfs = []
        for tile in tiles_in_dashboard:
            df = pd.read_json(sdk.run_inline_query(result_format='json',body = tile.query))
            # Apply a sorting to all columns, columns sorted in ascending order
            dfs.append(self.sort_all_columns(df))
        return dfs

if __name__ == '__main__':
    dev_branch = 'rr_testing_dev_vs_prod'
    project_name = 'looker_ssh'

    prod = LookerEnvironment('production')
    dev = LookerEnvironment('dev')
    dashboard = Dashboard('4')

    print("Testing Production:")
    print("First Tile from Production:")
    prod_tile = dashboard.get_all_tiles_data(prod.sdk)
    print(prod_tile[0],'\n')

    print("Testing Development:")
    # Step 1: Call method to switch to development 
    dev.switch_environment()
    # [Optional]: Output session method to confirm
    print(dev.get_session())
    # Step 2: Swap to the dev branch you want to test
    dev.checkout_dev_branch(project_name,dev_branch)
    print("First Tile from Development:")
    dev_tile = dashboard.get_all_tiles_data(dev.sdk)
    print(dev_tile[0])
