# PDF Data Extraction Service

## Instructions

Ensure Docker is installed, and in the project root run:

`docker compose build`

`docker compose up`

Once the service is running, access `localhost:8000/health` to ensure the container is running correctly.

Access the docs at `localhost:8000/docs` and in the `process-pdf` endpoint, click on the **Try it out**, upload the source PDF file and click on **Execute**.

The PDF can be found in `data/input/241025 Unicredit Macro & Markets Weekly Focus - python.pdf`.

This particular PDF file has the relevant information in the following pages:
- *Markets at a glance*: Page 2
- *Major data releases and economic events of the week ahead*: Page 3

These specific pages are set by default, so no need to change them for this particular PDF, but it may be necessary to do so for future reports.

## Assumptions/Challenges

## Thoughts

