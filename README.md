## Catfact DAG

### Prerequisites:
- Install [Docker](https://docs.docker.com/engine/install/)
- Install  [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli?tab=linux#install-the-astro-cli)

### Steps to run the project:
1) Run the following command to start the astro dev environment
```
astro dev init
```

2) Replase the content of the Dockerfile with the following content:
```
FROM quay.io/astronomer/astro-runtime:8.10.0
```

3) Start the astro dev environment with the following command:
```
astro dev start
```

4) Setup gcp connection with the Airflow UI
5) Stop the astro dev environment with the following command:
```
astro dev stop
```
