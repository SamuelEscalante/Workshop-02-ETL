<img src="https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/9a64daaf-b3ef-4f9e-ad01-28976b688ca4" alt="top_image" width="1000px" height="200px">

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
![image](https://github.com/SamuelEscalante/Workshop-02-ETL/assets/111151068/fe4d01ff-a54a-446d-869e-aa05ba0f4ca8)

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
#### ⚠️ Warning -> This proyect is made using a Unix OS, please take this in count ⚠️

#### Applications :
1. Install Python : [Python Downloads](https://www.python.org/downloads/)
2. Install PostgreSQL : [PostgreSQL Downloads](https://www.postgresql.org/download/)
3. Install Power BI : [Install Power BI Desktop](https://www.microsoft.com/en-us/download/details.aspx?id=58494) 

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

7. __Create your database__, this step is opcional if you are running locally, but if you are in the cloud you must have already your database


