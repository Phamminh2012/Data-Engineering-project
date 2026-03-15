import numpy as np
import pandas as pd
import json 
from datetime import datetime
def transform_mcf(json_path):
    with open(json_path,"r") as f:
        datas = json.load(f)

    results = []
    for data in datas:
      title = data["title"]
      description = data["description"]
      skills = data['skills']
      position = data["positionLevels"][0]["position"]
      createAt = data['metadata']['createdAt']
      createAt = datetime.fromisoformat(createAt.replace("Z", "+00:00")).strftime("%Y-%m-%d")


      postedCompany = data['postedCompany']['name']
      req_skill = []
      for skill in skills:
        req_skill.append(skill['skill'].lower()) 

      temp = {
          "title": title,
          "description": description,
          "skills": req_skill,
          "position": position,
          "createAt": createAt,
          "company": postedCompany,
          "source": "MCF"
      }
      results.append(temp)
       

    
    with open("/opt/airflow/data/processed/mcf_data_processed.json",'w') as f:
        json.dump(results,f,indent=4)

      
       

    

      