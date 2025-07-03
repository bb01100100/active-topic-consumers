# Find topic <-> consumer group mappings

Identify which Kafka topics are actively being consumed by which consmer groups.

1. Outputs human friendly summaries by group/topic to screen
2. Writes a json group -> topic mapping file so you can see which groups are consuming from a given topic
3. Writes a json topic -> group mapping file so you can see which topics a given group is consuming from.

```
usage: find-topic-consumers.py [-h] --config_file CONFIG_FILE --output_file OUTPUT_FILE

optional arguments:
  -h, --help            show this help message and exit

required arguments:
  --config_file CONFIG_FILE, -f CONFIG_FILE
                        Configuration file
  --output_file OUTPUT_FILE, -o OUTPUT_FILE
                        File to write group/topic info to (default is stdout)
```

Example output:

```
Found 4 consumer groups.

Describing consumer groups and their topic assignments:

--- Consumer Group: groupA ---
  State: STABLE
  Consumed Topics: source-topic

--- Consumer Group: groupB ---
  State: STABLE
  Consumed Topics: topic-b, topic-c

--- Consumer Group: groupC ---
  State: STABLE
  Consumed Topics: error-a, topic-b, source-topic

--- Consumer Group: groupD ---
  State: EMPTY
  No active members in this group.

Topics file is output-topics.json
Writing Topic->Groups mapping to output-topics.json file.
Writing Group->Topics mapping to output-groups.json file.

```

Example JSON output for topic-to-group mappings:

```
{
  "source-topic": [
    "groupA",
    "groupC"
  ],
  "topic-b": [
    "groupB",
    "groupC"
  ],
  "topic-c": [
    "groupB",
    "group2"
  ],
  "error-a": [
    "groupC"
  ]
}
```

Example JSON output for group-to-topic mappings:

```
{
  "groupA": [
    "source-topic"
  ],
  "groupB": [
    "topic-b",
    "topic-c"
  ],
  "groupC": [
    "error-a",
    "topic-b",
    "source-topic"
  ]
}
```
