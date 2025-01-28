# UFC-ETL
This is a automated data pipeline which extracts ufc data from ufcstats.com, cleans such data, stores such data in a database and aggregate such data. This data will then be used to train a machine learning model which is in my other repository. The data pipeline is automated via using apache airflow. I just hosted this data pipeline in AWS EC2.

This pipeline is created in python and using beautiful soup I scraped UFC data for ufc events, ufc fights in those events, ufc detailed fights(round by round fights) and finally fighter details. All these data were cleaned before sending to the database. The database used here is azure database. The fighters data were all also aggregated based on certain metrics which were dependant on the fights. All of this data is then sent to the machine learning model i created in the other repository.

The .env files contain the specific environment variables to connect to the database as well as hosting the apache airflow server. The machine learning model is deployed in amazon ECS using docker and more details including documentation can be found in the machine learning repo: https://github.com/Satveer27/UFC_ML_model
