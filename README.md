# PDF Data Extraction Service

## Instructions

### Simple Run:

In this service, we will:

>1. Parse the PDF and generate the appropriate dataframes for each table found in pages 2 and 3.
>2. From the **Markets at a glance** tables, consolidate into a single dataframe and extract the top and bottom 3 performers according to last year's data.
>3. Export the 5 tables found in **Markets at a glance** into their corresponding csv files.
>4. Store the data from **Markets at a glance** and **Major data releases and economic events of the week ahead** into their corresponding tables in a PostgreSQL database running inside a Docker container.

To run this script:

1. Ensure Docker and python 3 are installed, open a terminal window, and in the project root run:

    `docker compose build`

    `docker compose up`

    This will ensure the database is running and we can store the data.

2. Open a new terminal window in the same folder, create a virtual environment and install the requirements:

    `python -m venv .venv`

    `pip install -r requirements.txt`

3. To create the necessary tables in the database, we run:

    `docker compose exec app alembic upgrade head`

4. We run the script:

    `python app/simple_run.py`

    The csv files will now be created/updated in data/output. Since the files were already created, you can delete them and re-run the script to ensure the files are regenerated.
    
    To check the database, use any SQL client (e.g DBeaver), connect to `localhost:5432`, to the markets_weekly database as `user` with password: `pass`. You should see 7 tables with data inside.

The next step is to use the API to upload the PDF file and get a zipped folder with the csv files:

5. Go to a web browser to `localhost:8000/docs` and in the `process-pdf` endpoint, click on the **Try it out**, upload the source PDF file $^{[1]}$
 and click on **Execute**. You should receive a `200` response and a zipped folder to download. Unzipping the folder should present the csv files corresponding to the tables in pages 2 and 3.

$^{[1]}$ The PDF can be found in `data/input/241025 Unicredit Macro & Markets Weekly Focus - python.pdf`.

## Assumptions/Challenges



## Thoughts

