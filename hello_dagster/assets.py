import base64
import csv
import urllib.request
import zipfile
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
from dagster import (
    AssetSelection,
    Definitions,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    build_asset_reconciliation_sensor,
    define_asset_job,
    get_dagster_logger,
)
from wordcloud import STOPWORDS, WordCloud


@asset
def story_ids():
    top_story_ids = requests.get(
        "https://hacker-news.firebaseio.com/v0/topstories.json", verify=False
    ).json()
    return top_story_ids[:10]


@asset
def top_stories(story_ids):
    logger = get_dagster_logger()
    results = []

    for item_id in story_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json", verify=False
        ).json()
        results.append(item)

        if len(results) % 2 == 0:
            logger.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    return Output(
        value=df,
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        },
    )


@asset
def stopwords_zip() -> None:
    """Zip file that contains stopwords."""
    urllib.request.urlretrieve(
        "https://docs.dagster.io/assets/stopwords.zip",
        "stopwords.zip",
    )


@asset(non_argument_deps={"stopwords_zip"})
def stopwords_csv() -> None:
    """csv version of the raw stopwords"""
    with zipfile.ZipFile("stopwords.zip", "r") as zip_ref:
        zip_ref.extractall(".")


@asset(non_argument_deps={"stopwords_csv"})
def topstories_word_cloud(top_stories):
    """Beeg word cloud"""
    with open("stopwords.csv", "r") as f:
        stopwords = {row[0] for row in csv.reader(f)}

    titles_text = " ".join([str(item) for item in top_stories["title"]])
    titles_cloud = WordCloud(stopwords=stopwords, background_color="white").generate(titles_text)

    # Generate the word cloud image
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(titles_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    return Output(value=image_data, metadata={"plot": MetadataValue.md(md_content)})


# @asset
# def a():
#     pass


# @asset
# def b(a):
#     pass


# update_job = define_asset_job(name="update_job", selection=AssetSelection.keys("a"))

# update_sensor = build_asset_reconciliation_sensor(
#     name="update_sensor", asset_selection=AssetSelection.all()
# )

# update_job_schedule = ScheduleDefinition(
#     name="update_job_schedule", job=update_job, cron_schedule="* * * * *"
# )

defs = Definitions(
    assets=[a, b],
    schedules=[update_job_schedule],
    sensors=[update_sensor],
)
