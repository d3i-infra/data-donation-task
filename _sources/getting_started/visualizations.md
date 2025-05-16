# Adding data visualizations

You can add data visualizations to the consent form page, that will be shown below a data table. These visualizations will dynamically aggregate and visualize the data, responding to search queries and deleted items.

Good visualizations can help participants to see and explore what data they are about to donate, and thereby support informed consent. Furthermore, it can make the data donation process more educational and enjoyable.

## Adding visualizations to tables

Visualizations are always directly connected to a **consent form table**. When in script.py you create a consent form table, you can implement visualizations as follows:

```python
table_title = props.Translatable({
    "en": "Table title",
    "nl": "Tabel titel"
})

table = props.PropsUIPromptConsentFormTable(
    id = "tableId",
    title = table_title,
    data_frame = df,
    visualizations = [])
```

You can now add one or multiple **visualization specifications** to the `visualizations` list.

## Visualization Specification

A visualization specification provides instructions for creating a visualization based on the data in the table. This visualization will then be created dynamically, so that when the table is updated (e.g., when participants search the data or remove rows) the visualization is updated as well.

A specification covers three main components:

- **Aggregation**: How should the table data be aggregated. e.g., count the number of rows per day
- **Display**: How should the aggregated data be displayed? e.g., line chart, bar chart, wordcloud
- **Labels**: Any labels to help along interpretation, optionally with translations (as seen above in the table_title)

A detailed explanation of the visualizatoin specification is shown below in the **Specification Guide**. But we recommend first having a look at the following examples.

## Examples

Say we have data about every time a participant viewed a certain channel, and we also also know the channel category (e.g., sports, entertainment) and the exact timestampe. We have put this in a `data_frame` with the columns: **channel**, **category** and **timestamp**. We can then make a number of different visualizations.

### Categorical variables | Bar chart of views per category

```python
vis1 = dict(
    title = dict(en= "views per category", ...),
    type = "bar",
    group = dict(column = "category", label = "Category")
    values = [dict(aggregate = "count", label = dict(en = "number of views", ...))]
)
```

The **type** determines the chart type, and can in this case be "bar","line" or "area". The **group** determines how the data should be grouped and aggregated, which in this case is per category. The **values** determines the values to calculate per group, which here is just the count of the rows.

**!!!** Notice that `values` is a list, and not a single dictionary. Adding multiple value dictionaries will create multiple y-values, for grouped barcharts or multiple lines or areas.

The **label**'s can be either a single _string_ (as in the `group`) or a dictionary with different languages, where keys are country codes, and values are labels (as in the `values`).

### Date variables | Area chart of views per month

```python
vis2 = dict(
    title = dict(en= "views over time", ...),
    type = "area",
    group = dict(column = "timestamp", dateFormat = "month", label = "Month")
    values = [dict(aggregate = "count", label = dict(en = "number of views", ...))]
)
```

In this area chart (i.e. a line chart where the area below the line is coloured) we group the data by month, and use the same aggregation values as in the previous example to count the number of views per group.

The **dateFormat** grouping variable can be set if the column is a date string in ISO format: `YYYY-MM-DD` for date or `YYYY-MM-DD HH:MM:SS` for datetime (You can also use `YYYY-MM-DDTHH:SS:MM)`, but that doesn't look niced in the table).

The following formats are supported:

- **Fixed interval**: "year", "quarter", "month", "day", "hour"
- **Automatic interval**: "auto" will pick an interval based on the min/max date. Pick this if the min/max date can vary heavily between participants. This also avoids slowing down the application by accidentally generating a huge graph (e.g., a one year period with "hour" interval)
- **cycles / season**: "month_cycle" (January - December), "weekday_cycle" (Monday - Sunday) and "hour_cycle" (1 - 24).

### Second-level aggregation | Line chart of views over time per category

Above we mentioned that you can add multiple values to create multiple y-values. But this only works if your data is _wide_. Alternatively, you can also perform a second-level aggregation on _long_ data.

```python
vis3 = dict(
    title = dict(en= "views per category over time", ...),
    type = "line",
    group = dict(column = "timestamp", dateFormat = "auto", label = "Month")
    values = [dict(
        aggregate = "count",
        label = dict(en = "number of views", ...),
        group_by = "category"
    )]
)
```

Here we changed three things. First, we changed the type to "line", because that's a bit easier on the eye with multiple y-values. Second, we added `group_by` to the aggregation value, setting it to "category". This will break the values data into groups for categories, and calculate the aggregation statistic per category. This will be visualized as a line chart where the frequency of each category (e.g., sport, entertainment) will be displayed on separate lines.

A third change is that we set the dateFormat to "auto" instead of fixing it to "month". This will automatically pick a suitable time interval based on the range of column (last date - first date). This could mean that different participants see different intervals, depending on what works best for their own data.

### Text variables | A wordcloud

As a final example, we'll look at a different sub-specification for visualizing textual data. We'll make a wordcloud of channels, based on their frequency in the data.

```python
vis4 = dict(
    title = dict(en= "Most viewed channels", ...),
    type = "wordcloud",
    textColumn = 'channel',
    tokenize = False,
)
```

This creates a wordcloud of the full channel names. Note that we could also have tokenized the texts, but for channels (e.g., YouTube channels) the full names are probably most informative.

## Example wrap-up

Now that we have created visualizations, we can add them to the consent form table. Note that above we assigned our specifications to **vis1** to **vis4**. We can now simply add them to the visualiations list.

```python
table = props.PropsUIPromptConsentFormTable(
    id = "tableId",
    title = table_title,
    data_frame = df,
    visualizations = [vis1, vis2, vis3, vis4])
```

## Specification guide

This is an overview of the visualiation specification. First, there are some **general visualization arguments** that every visualization has. Second, there are specific arguments depending on the visualization **type**

### General visualization arguments

Every visualization has the following arguments

- **title**: A title for the visualization. This has to be a translation dictionary (see **translation** spec below)
- **type**: The type of the visualization. The type determines what specification you need to follow
  - **Chart visualiation**: "line", "bar" or "area"
  - **Text visualization**: "wordcloud"
- **height (optional)**: The height of the chart in pixels

### Chart visualization arguments

Chart visualizations work by aggregating the data into X, Y and optionally Z axes. It's the basis for most common charts.

- **type**: "line", "bar" or "area"
- **group**: specifies the column to group and aggregate the data by. The group is visualized on the x-axis.
  - **label**: x-axis label. Either a string or translation dictionary (see **translation** spec below)
  - **column**: the name of the column
  - **dateFormat (optional)**: if column is a date, select how it should be grouped. (see **dateFormat** spec below)
  - **levels (optional)**. A list of strings with the specific column values to use. This also makes sure these values are displayed if they are missing in a participants data (also see **values** -> **addZeroes**)
- **values**: A list (**!!**) of objects. Each object specifies an (aggregate) value to calculate per group. A value is visualized on the y-axis. Multiple values can be given for multiple y-values
  - **label**: y-axis label. Either a string or translation dictionary (see **translation** spec below)
  - **column (optional)**: the column based on which the value is calculated. Can be empty if just counting rows.
  - **aggregate**: The aggregation function. (see **aggregate** spec below)
  - **addZeroes**: Boolean. If true, add zeroes for empty groups. If **levels** are specified, participants will explicitly see that they occured zero times in their data. If **dateFormat** is used, this fills possible gaps (note that this mostly makes sense for row "count" aggregations where absense implies zero)
  - **group_by (optional)**: the name of a column to do a second-level aggregation. This will create multiple y-values where the value in the column becomes the label.

### Text visualization arguments

Text visualizations take a text column as input.

- **type**: "wordcloud"
- **textColumn**: A text (string) column in the data
- **tokenize (optional)**: Boolean. If true, the text will be tokenized
- **valueColumn (optional)**: By default, every text or token will be given a value based on the number of rows in which it occurs. Alternatively, you can specify a numeric column, in which case (the sum of) the values in this column will be used.
- **extract (optional)**: Normally, all preprocessing of the data should be handled in the import scripts, but for convenience we will provide some common methods for extracting parts of a string. Currently supports:
  - "url_domain": If the column contains URLs, extract only the domain.

### Spec details

Here are some details for the more complicated spec components.

#### - translation

A translation dictionary has country codes as keys and the translations as values: `dict(en = "english label", nl = "dutch label")`. (This is identical to the dictionary used in the `props.Translatable`)

#### - dateFormat

If column is a date (`YYYY-MM-DD`, `YYYY-MM-DD HH:MM` or `YYYY-MM-DD HH:MM:SS`), select how the date is grouped. options are:

- **Fixed interval**: "year", "quarter", "month", "day", "hour"
- **Automatic interval**: "auto" will pick an interval based on the min/max date. Pick this if the min/max date can vary heavily between participants. This also avoids slowing down the application by accidentally generating a huge graph (e.g., a one year period with "hour" interval)
- **cycles / season**: "month_cycle" (January - December), "weekday_cycle" (Monday - Sunday) and "hour_cycle" (1 - 24).

#### - aggregate

The function by which to aggregate the column in `values`. The following functions are currently supported

- "count" just counts the rows
- "mean" and "sum" require the value column to be numeric.
- "count_pct" gives the count as a percentage of the total number of rows.\*
- "pct" sums the values of a numeric column and divides by the total sum.\*

**\*** _If a secondary aggregation is used, percentages are calculated within the primary aggregation group_
