# What is this

This is an Apache Spark demonstration that creates a visualization of Wikipedia's hourly page views. The pageview data is quite large so it's perfect for showing how it can run on a distributed system like Spark. It's still a small example, just running on a cluster with 3 workers. I made a dashboard that shows what was computed, and a live view that shows what the cluster is doing while it runs.

# Requirements

- Install docker desktop
- Install python 3

# How to Run

Run `docker compose up -d`
Go to `http://localhost:8080/`. It should show 3 alive workers
You can see the live job UI while it's running by going to `http://localhost:4040/`
The finished job history is at `http://localhost:18080/`

Run this (probably different command for Mac)

```bash
   docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events --conf spark.executor.cores=1 /opt/jobs/export_dashboard_data.py
```

Then quickly in another terminal go run this

```
python live_view.py
```

After it's all finished you can open dashboard.html in a browser and upload `jobs/dashboard_data.json`.

# Screenshots

This is what the live view will look like
<img width="1923" height="1049" alt="image" src="https://github.com/user-attachments/assets/6c70b550-c7fe-44bb-89b3-e80b218e8878" />

This is what the data dashboard will look like
<img width="1575" height="1465" alt="image-1" src="https://github.com/user-attachments/assets/c0cc36d7-c61a-432c-b85f-1309d50b2c20" />
