## Q1: Can't login to airflow

Solution:
1.enter the webserver container
2.run the command:
```
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```

