# IS3107 Data Engineering

## What is it?

This ingests job portal data from two sources:
- RapidAPI
- MyCareersFuture

We did try the following, but we dropped it:
- Telegram (because it did not have a good enough schema)
- AdZuma (because it did not provide the full job description)
- Reed.co.uk (because the API was behind CloudFlare despite it being an API)

How do you install?

Very simple: Just build the docker file. Just:
```
docker compose up --build
```

## What exactly do we want to get out of it?
I don't know.

Top 5 skills?

How many job postings?

A lot of things you can do. But we have the raw output that we did a bit of transformation, so you can do whatever you want.