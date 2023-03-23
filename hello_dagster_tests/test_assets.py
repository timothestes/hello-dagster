import pandas as pd
from dagster import materialize

from hello_dagster.assets import story_ids, top_stories, topstories_word_cloud


def test_hackernews_assets():
    assets = [story_ids, top_stories, topstories_word_cloud]
    result = materialize(assets)
    assert result.success
    df = result.output_for_node("top_stories")
    assert len(df) == 10


def test_topstories_word_cloud():
    df = pd.DataFrame(
        [
            {
                "title": "Wow, Dagster is such an awesome and amazing product. I can't wait to use it!"
            },
            {"title": "Pied Piper launches new product"},
        ]
    )
    results = topstories_word_cloud(df)
    assert results is not None  # It returned something
