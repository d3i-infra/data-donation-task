# Adding data visualizations

Visualizations appear on the consent form below their corresponding table. They aggregate and display the data dynamically, updating in response to participant actions such as search queries or row deletions.

Good visualizations help participants understand what they are about to donate, supporting informed consent. They can also make the donation process more engaging and informative.

## How to add a visualization

Visualizations are defined in the `Table config::` JSON block inside each extractor function's docstring — the same block that sets the table title, description, and column headers. Add a `"visualizations"` list to that block:

```python
def my_data_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract my data.

    Table config::

        {
          "id": "myplatform_my_data",
          "title": {"en": "My data", "nl": "Mijn data"},
          "description": {"en": "...", "nl": "..."},
          "headers": {
            "channel":   {"en": "Channel",   "nl": "Kanaal"},
            "category":  {"en": "Category",  "nl": "Categorie"},
            "timestamp": {"en": "Timestamp", "nl": "Tijdstip"}
          },
          "visualizations": [
            {
              "title": {"en": "Views per category", "nl": "Weergaven per categorie"},
              "type": "bar",
              "group": {"column": "category", "label": "Category"},
              "values": [{"aggregate": "count", "label": {"en": "Number of views", "nl": "Aantal weergaven"}}]
            }
          ]
        }
    """
    ...
```

When `pnpm generate-config` is run, these blocks are read and written into the platform config file. At runtime, the framework reads the config and renders the visualizations automatically — no additional Python code is needed.

Multiple visualizations can be included in the list. They are displayed in order below the table.

---

## Examples

The following examples use a DataFrame with columns `channel`, `category`, and `timestamp`, where each row represents a single video view.

### Bar chart — categorical variable

```json
{
  "title": {"en": "Views per category", "nl": "Weergaven per categorie"},
  "type": "bar",
  "group": {"column": "category", "label": "Category"},
  "values": [{"aggregate": "count", "label": {"en": "Number of views", "nl": "Aantal weergaven"}}]
}
```

`type` can be `"bar"`, `"line"`, or `"area"` for chart visualizations. `group` sets the column used for the x-axis. `values` is a list — each entry defines one y-value. Here there is one: a row count per group.

Note that `values` is always a list. Adding multiple entries creates grouped bar charts or multi-line charts.

### Area chart — date variable

```json
{
  "title": {"en": "Views over time", "nl": "Weergaven over tijd"},
  "type": "area",
  "group": {"column": "timestamp", "dateFormat": "month", "label": "Month"},
  "values": [{"aggregate": "count", "label": {"en": "Number of views", "nl": "Aantal weergaven"}}]
}
```

When the grouped column contains dates, set `dateFormat` to control the grouping interval. The column should be an ISO date string (`YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`).

Supported formats:

- **Fixed interval**: `"year"`, `"quarter"`, `"month"`, `"day"`, `"hour"`
- **Automatic**: `"auto"` — picks an interval based on the min/max date range. Useful when date ranges vary significantly between participants, and avoids accidentally generating very large charts.
- **Cyclic**: `"month_cycle"` (January–December), `"weekday_cycle"` (Monday–Sunday), `"hour_cycle"` (0–23)

### Line chart — second-level aggregation

```json
{
  "title": {"en": "Views per category over time", "nl": "Weergaven per categorie over tijd"},
  "type": "line",
  "group": {"column": "timestamp", "dateFormat": "auto", "label": "Month"},
  "values": [
    {
      "aggregate": "count",
      "label": {"en": "Number of views", "nl": "Aantal weergaven"},
      "group_by": "category"
    }
  ]
}
```

Adding `group_by` to a value entry performs a second-level aggregation. The data is split by the values in that column, and a separate line is drawn for each group. This works on long-format data where the categories are in a column rather than spread across multiple columns.

### Word cloud — text variable

```json
{
  "title": {"en": "Most viewed channels", "nl": "Meest bekeken kanalen"},
  "type": "wordcloud",
  "textColumn": "channel"
}
```

Word clouds take a text column as input and size each term by its frequency. `tokenize` can be set to `true` to split text into individual words; omitting it (or setting it to `false`) treats the full cell value as a single term. See `example.py` for a working instance of this visualization type.

---

## Specification reference

### General arguments

Every visualization requires:

- **`title`**: Translation dictionary — `{"en": "...", "nl": "..."}`.
- **`type`**: `"bar"`, `"line"`, `"area"`, or `"wordcloud"`.
- **`height`** *(optional)*: Chart height in pixels.

### Chart visualization (`"bar"`, `"line"`, `"area"`)

- **`group`**: Object specifying the x-axis column.
  - **`column`**: Column name.
  - **`label`**: Axis label — string or translation dictionary.
  - **`dateFormat`** *(optional)*: Date grouping format (see above).
  - **`levels`** *(optional)*: List of specific values to include on the axis, ensuring absent values still appear (e.g. as zero).
- **`values`**: List of value objects — each defines one y-axis series.
  - **`label`**: Axis label — string or translation dictionary.
  - **`column`** *(optional)*: Column to aggregate. Can be omitted for row counts.
  - **`aggregate`**: Aggregation function (see below).
  - **`addZeroes`** *(optional)*: Boolean. Fill empty groups with zero.
  - **`group_by`** *(optional)*: Column for second-level aggregation, producing one series per unique value.

### Word cloud visualization (`"wordcloud"`)

- **`textColumn`**: Column containing the text to visualize.
- **`tokenize`** *(optional)*: Boolean. Split values into individual tokens.
- **`valueColumn`** *(optional)*: Numeric column whose values determine word size, instead of row frequency.
- **`extract`** *(optional)*: Preprocessing shortcut. Currently supports `"url_domain"` to extract the domain from a URL column.

### Aggregation functions

Used in the `aggregate` field of a chart value:

- `"count"` — number of rows in the group.
- `"mean"` — mean of the value column (numeric).
- `"sum"` — sum of the value column (numeric).
- `"count_pct"` — row count as a percentage of total rows.*
- `"pct"` — column sum as a percentage of the total sum.*

*When `group_by` is used, percentages are calculated within the primary group, not across the full dataset.
