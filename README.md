# PDF Data Extraction Service

## Instructions

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


3. To create the necessary tables in the database, open a new terminal and run:

    `docker compose exec app alembic upgrade head`

4. We run the script:

    `docker compose exec app python simple_run.py`

    The csv files will now be created/updated in data/output. Since the files were already created, you can delete them and re-run the script to ensure the files are regenerated.
    
    To check the database, use any SQL client (e.g DBeaver), connect to `localhost:5432`, to the markets_weekly database as `user` with password: `pass`. You should see 7 tables with data inside. Run `select * from major_events` and check that there are a total of 48 rows.

The next step is to use the API to upload the PDF file and get a zipped folder with the csv files:

5. Go to a web browser to `localhost:8000/docs` and in the `process-pdf` endpoint, click on the **Try it out**, upload the source PDF file $^{[1]}$
 and click on **Execute**. You should receive a `200` response and a zipped folder to download. Unzipping the folder should present the csv files corresponding to the tables in pages 2 and 3.

$^{[1]}$ The PDF can be found in `data/input/241025 Unicredit Macro & Markets Weekly Focus - python.pdf`.

## Assumptions/Challenges

The main challenge of the implementation was figuring out how to parse the PDF.

Initially I used a library called PyMUPDF, but it was incapable of finding the data in pages 2 and 3, it only found the headers. Upon investigation of other PDF libraries, I found Camelot and Tabula, which apparently have a better time parsing table data from PDFs. I ultimately decided on Camelot, as the amount of parameters available to experiment with seemed promising.

To read the tables themselves I had to tweak the parameters several times until reaching an appropriate setup to create dataframes that weren't completely broken. The row tolerance parameter in the parse step is different for pages 2 and 3, as can be seen in `app/pdf_tables_parser.py`. I assume it will have to be different for each table in the file, as the formats are slightly different.

A lot of extraction steps and cleanup had to be done for these tables, from removing data that weren't in the actual tables (Camelot extracted the phrase *Source: Bloomberg, UniCredit Group Investment Strategy* into the table in page 3), to removing decimal points and ensuring numerical data are converted to numerical columns.

Setting up the database and the Docker container was comparatively easier to this data extraction step, which required a lot of experimentation to get right.

## Thoughts

I managed to find a couple of PDFs from other weeks (found in the `data/input` folder), and while they are is similar to the one provided in the test from October (though some tables are slightly different sizes), it's clear that this document can change at any moment, which makes the whole implementation very frail and prone to fail. 

As a matter of fact, while the document from September 2024 can be uploaded to the API to get the zipped folder with the CSVs, the one from December 2023 fails in the parsing step, which means each of these PDFs would need close, manual inspection.

Ideally, work would need to be done to investigate other potential, more stable sources for this data to avoid manual work to extract the data each week. If it were absolutely necessary to maintain this source, I would dedicate a lot of time to reduce the pain points of extracting, but without knowing exactly how UniCredit will deliver the PDF, it's prone to be a very finnicky process.
