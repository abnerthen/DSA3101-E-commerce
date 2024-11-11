## Application setup

Because of the way this dashboard is set up, it requires data fetched from BigQuery. You will need to create a `.env` file that consists of the following items:

- `GCP_PKEY` (Google Cloud Private Key)
- `PKEY_ID` (Private Key's ID)
- `PROJ_ID` (Project ID)
- `CLIENT_EMAIL` (Client email)
- `client_x509_cert_url` (some URL)

## How to run the application

First, build the image. On Docker desktop, navigate to the project directory, and run this in the terminal.

```
docker build -t my-app:1.0 -f src/Dockerfile .
```

Next, you will want to run the container after building it. Just go to "Images", select the container and click 'Run'.
**Specify host port as '0'.**

After it runs, just click the link below the name of the application and the dashboard should open.