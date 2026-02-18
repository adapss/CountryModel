import os
from dotenv import load_dotenv
import pandas as pd

import msal
import requests
from jinja2.optimizer import Optimizer

class MSGraphTokens:

    def __init__(self):
        self.TENANT_ID = os.getenv("TENANT_ID", "47f9f66c-31db-442a-8089-f71ee5b04d2b")
        self.CLIENT_ID = os.getenv("CLIENT_ID", "2e4d0eb0-014e-4f1f-ab18-ec0fbb620035")
        self.CLIENT_SECRET = os.getenv("CLIENT_SECRET", "hlD8Q~7AmOT7yqgSMYO0ej-SZ6KEkGsmBslmWb08").strip()


    def generate_access_token(self):
        authority = f"https://login.microsoftonline.com/{self.TENANT_ID}"
        scope = ["https://graph.microsoft.com/.default"]
        app = msal.ConfidentialClientApplication(
            self.CLIENT_ID,
            authority=authority,
            client_credential = self.CLIENT_SECRET,
        )
        token_response = app.acquire_token_for_client(scopes=scope)
        access_token = token_response["access_token"]
        return access_token

class ProductDescriptionTable:

    def __init__(self, msal_token):
        self.access_token = msal_token

    def technology_group_list_via_graph(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        site_url = "https://graph.microsoft.com/v1.0/sites/arcadvisorygroup.sharepoint.com:/sites/StudyCentral"
        response = requests.get(site_url, headers=headers)
        site_id = response.json()["id"]
        list_items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/DV_TechnologyDomains/items?expand=fields"
        response = requests.get(list_items_url, headers=headers)
        field_items = response.json()["value"]
        data = []
        for item in field_items:
            fields = item.get("fields", {})
            data.append({
                "TechnologyGroup": fields.get("Title"),
                "ID": fields.get("id")
            })

        df = pd.DataFrame(data)
        default_name, default_id = self.get_default_technology_group()
        default_technology_group = pd.DataFrame([{'TechnologyGroup': default_name, 'ID': default_id}])
        df = pd.concat([df, default_technology_group], ignore_index=True)
        return df    # df return is 2 col [tech_name, id]


    def lookup_technology_group_id(self,tech_group_name:str):
        tg = self.technology_group_list_via_graph()
        tg = tg[tg['TechnologyGroup'] == tech_group_name]
        tg_id = tg['ID'].iloc[0]
        return tg_id

    # The default technology group will always be in the Economic Model.  ID=0 is never applied in the sharepoint list as an assignment to a
    # defined technology group so this is a safe assignement.
    def get_default_technology_group(self):
        return "Default",0

    def market_study_list_via_graph(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        site_url = "https://graph.microsoft.com/v1.0/sites/arcadvisorygroup.sharepoint.com:/sites/StudyCentral"
        response = requests.get(site_url, headers=headers)
        site_id = response.json()["id"]
        list_items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/ST_ProductDescription/items?expand=fields"
        response = requests.get(list_items_url, headers=headers)
        field_items = response.json()["value"]
        market_studies = []
        for item in field_items:
            fields = item.get("fields", {})
            market_studies.append({
                "Study": fields.get("Title"),
                "TechnologyDomainID": fields.get("TechnologyDomainLookupId"),
                "ID": fields.get("id")
            })
        tech_groups = self.technology_group_list_via_graph()
        reports = pd.DataFrame(market_studies)
        tg = pd.DataFrame(tech_groups)
        reports = pd.merge(reports, tech_groups, left_on='TechnologyDomainID', right_on='ID', how='inner')
        reports = reports.drop(['ID_x', 'ID_y'], axis=1).sort_values(by='TechnologyGroup', ascending=True)
        return reports

    def get_market_study_technology_group(self, market_study:str):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        site_url = "https://graph.microsoft.com/v1.0/sites/arcadvisorygroup.sharepoint.com:/sites/StudyCentral"
        response = requests.get(site_url, headers=headers)
        site_id = response.json()["id"]
        list_items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/ST_ProductDescription/items?expand=fields"


        # response = requests.get(list_items_url, headers=headers)
        # field_items = response.json()["value"]
        all_items = []
        while list_items_url:
            r = requests.get(list_items_url, headers=headers)
            r.raise_for_status()
            data = r.json()
            batch = data.get("value", [])
            all_items.extend(batch)
            list_items_url = data.get("@odata.nextLink")  # follow pagination

        marketstudy_x_technology = []
        for item in all_items:
            fields = item.get("fields", {})
            marketstudy_x_technology.append({
                "Study": fields.get("Title"),
                "TechnologyDomainID": fields.get("TechnologyDomainLookupId"),
            })
        tech_groups = self.technology_group_list_via_graph()
        report_x_technology = pd.DataFrame(marketstudy_x_technology)
        report_x_technology = report_x_technology.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        market_study = market_study.strip()
        report_x_technology = report_x_technology[report_x_technology["Study"] == market_study]
        tg = pd.DataFrame(tech_groups)
        merged = pd.merge(report_x_technology, tg, left_on='TechnologyDomainID', right_on='ID', how='left')
        tg_name = merged["TechnologyGroup"].iloc[0] if not merged.empty else None
        tg_id = merged["ID"].iloc[0] if not merged.empty else None
        return tg_name, tg_id
















