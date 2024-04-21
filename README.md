![image](https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/7565a539-3dcd-48d4-9cc8-229d6922dd3d)

Presented by Samuel Escalante Gutierrez - [@SamuelEscalante](https://github.com/SamuelEscalante)

### Tools used

- **Python** <img src="https://cdn-icons-png.flaticon.com/128/3098/3098090.png" alt="Python" width="21px" height="21px">
- **Jupyter Notebooks** <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Jupyter_logo.svg/883px-Jupyter_logo.svg.png" alt="Jupyer" width="21px" height="21px">
- **PostgreSQL** <img src="https://cdn-icons-png.flaticon.com/128/5968/5968342.png" alt="Postgres" width="21px" height="21px">
- **Power BI** <img src="https://1000logos.net/wp-content/uploads/2022/08/Microsoft-Power-BI-Logo.png" alt="PowerBI" width="30px" height="21px">
- **SQLAlchemy** <img src="https://quintagroup.com/cms/python/images/sqlalchemy-logo.png/@@images/eca35254-a2db-47a8-850b-2678f7f8bc09.png" alt="SQLalchemy" width="50px" height="21px">
- **Apache Airflow** <img src="https://static-00.iconduck.com/assets.00/airflow-icon-512x512-tpr318yf.png" alt="Airflow" width="30px" height="25px">
---
### Workflow
![Workshop-2-Workflow](https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/b5415264-e1ab-486f-9390-9ee265120066)

---
### About the data

The datasets used in this project were obtained, respectively, from:

- [🏆Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards) "Grammy Awards, 1958 - 2019"
- [🎹 Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)  "A dataset of Spotify songs with different genres and their audio features"

---
### Project organization

![tree (1)](https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/dab96c3e-34dc-4d31-97e9-aa6fae230180)

---
### Prerequisites
#### ⚠️ __Warning__ The pipeline was build using a Unix OS, please take this in count ⚠️

#### Applications :
1. Install Python : [Python Downloads](https://www.python.org/downloads/)
2. Install PostgreSQL : [PostgreSQL Downloads](https://www.postgresql.org/download/)

#### ⚠️ The data visualization was designed on Windows OS, taking advantage of the database instance in the cloud. ⚠️ 
4. Install Power BI : [Install Power BI Desktop](https://www.microsoft.com/en-us/download/details.aspx?id=58494) 

#### Preparations:
1. Get drive credentials, I recommend going to the [official PyDrive documentation](https://pythonhosted.org/PyDrive/) and you can use this [video](https://www.youtube.com/watch?v=ZI4XjwbpEwU&t=6s) as a guide.
2. If you want to run the project with a database instance with google cloud platform, use this [video](https://www.youtube.com/watch?v=hjzDMjo9Fko) as a guide or you can run the whole project locally!

---

### ¿How to run this project?  

1. Clone the project
```bash
  git clone https://github.com/SamuelEscalante/Workshop-02-ETL.git
```

2. Go to the project directory
```bash
  cd Workshop-2-ETL
```

3. In the root of the project, create a `db_settings.json` file, this to set the database credentials
```json
{
  "DIALECT": "The database dialect or type. In this case, it is set to 'postgres' for PostgreSQL.",
  "PGUSER": "Your PostgreSQL database username.",
  "PGPASSWD": "Your PostgreSQL database password.",
  "PGHOST": "The host address or IP where your PostgreSQL database is running.",
  "PGPORT": "The port on which PostgreSQL is listening.",
  "PGDB": "The name of your PostgreSQL database."
}
```

4. Create virtual environment for Python
```bash
  python -m venv venv
```

5. Activate the enviroment
```bash
  source venv/bin/activate 
```

6. Install libreries
```bash
  pip install -r requirements.txt
```

7.  Create a `.env` file and add this variable:
   
    - WORK_PATH <- Sets the working directory for the application, indicating the base path for performing operations and managing files.
    - DAG_PATH <- Set the dag path, indicate the path of the dag folder
    - EMAIL <- Set your email address, this for the email that will send the notifications
      
9. __Create your database__, this step is opcional if you are running locally, but if you are in the cloud you must have already your database

10. Start with the notebooks:
- 001_grammy_awards.ipynb
- 002_spotify_dataset.ipynb

11. Start Airflow:
    
    - Export to airflow your current path
      ```bash
      export AIRFLOW_HOME=${pwd}
      ```
    - Run apache airflow
      ```bash
      airflow standalone
      ```
    - Then go to your browser a search 'localhost:8080'
   
    - Finally trigger the dag

This is how the dag should look like    
![image](https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/a38c8efc-799a-4c1e-b7a6-25f667971dba)

12. Then go to Postgres and check if the table has been created successfully

13. Taking advantage of the database instance, go to Power Bi :  
13.1 Open a new dashboard
![image](https://github.com/SamuelEscalante/Workshop-01-ETL/assets/111151068/56e0c10a-4819-4e1e-ad28-d47ad4381ec2)

13.2 Look for 'Database PostgreSQL' :  
![image](https://github.com/SamuelEscalante/Workshop-01-ETL/assets/111151068/581e84ff-0cb4-4240-a998-217c8b9607c4)

13.3 Insert your information and accept :  
![image](https://github.com/SamuelEscalante/Workshop-01-ETL/assets/111151068/da2e0d78-ecdc-4068-a97b-c470911c4a18)
  
13.4 Select the tables and load the data :  
![image](https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/b32389a3-fbce-435d-9b9b-1d4cb05f5359)

See my dashboard [here]() 

The data is now synchronized with power bi! You can do your own dashboard. 


## Farewell and Thanks

Thank you for visiting our repository! We hope you find this project useful. If it has been helpful or you simply liked it, consider giving the repository a star! 🌟

We would love to hear your feedback, suggestions, or contributions.

Thanks for your support! 👋
