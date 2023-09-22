CREATE TABLE channel (
    channel_id UUID,
    subscribers Int32,
    channel_title String
) ENGINE = MergeTree()
ORDER BY channel_id;

CREATE TABLE date (
    date_id UUID,
    publish_date Date
) ENGINE = MergeTree()
ORDER BY date_id;

CREATE TABLE query (
    query_id UUID,
    query String
) ENGINE = MergeTree()
ORDER BY query_id;

CREATE TABLE video (
    video_id UUID,
    channel_id UUID,
    date_id UUID,
    duration String,
    query_id UUID,
    ENGINE = MergeTree()
    ORDER BY (video_id, channel_id, date_id, query_id)
) ENGINE = MergeTree()
ORDER BY video_id;