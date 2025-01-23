# Creating your own data donation task


After you have forked or cloned and installed the repository you can start creating your own donation task. 

You can create your own study by changing and/or adapting the code in the following directory `port/src/framework/processing/py/port/`
This directory contains the following files:

* `script.py`: Contains your donation task logic; which screen the participants will see and in what order
* `api/props.py`: Contains all the UI elements you can use; you can use this file for reference
* `api/commands.py`: Contains the Render and the Donate commands
* `main.py`: The main driver of you donation task, you don't need to touch this file

### `script.py`

`script.py` is the most important file and the one we will be working with the most

Lets look at a full example of a `script.py`. In this example we will be extracting data from a fictional DDP. 
Participants are asked to submit a zip file (any zip file will do in this case), and we will extract the file names and some meta data from this zip file. 
In a real study you would extract something substantial from the data.

`script.py` must contain a function called `process` this function determines the whole data donation task from start to finish (Which screens the participant will see and in what order, and what kind of data extraction will take place). 
At the time of writing this example is also the default `script.py`. 

In this example process defines the following data donation task:

1. Ask the participant to submit a zip file
2. Perform validation on the submitted zip file, if not valid return to step 1
3. Extract the data from the submitted zip file
4. Render the extract data on screen in a table
5. Send the data to the data storage upon consent

Although these can vary per data donation task, they will be more or less similar.

Below you can find the annotated example `process` function: 

```python
# script.py
import port.api.props as props
from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

import pandas as pd
import zipfile

def process(session_id: str):
    platform = "Platform of interest"

    # Start of the data donation task
    while True:
        # Ask the participant to submit a file
        file_prompt = generate_file_prompt(platform, "application/zip, text/plain")
        file_prompt_result = yield render_page(platform, file_prompt)

        # If the participant submitted a file: continue
        if file_prompt_result.__type__ == 'PayloadString':

            # Validate the file the participant submitted
            # In general this is wise to do 
            is_data_valid = validate_the_participants_input(file_prompt_result.value)

            # Happy flow (all is well):
            # The file the participant submitted is valid
            if is_data_valid == True:

                # Extract the data you as a researcher are interested in, and put it in a pandas DataFrame
                # Show this data to the participant in a table on screen
                # The participant can now decide to donate
                extracted_data = extract_the_data_you_are_interested_in(file_prompt_result.value)
                consent_prompt = generate_consent_prompt(extracted_data)
                consent_prompt_result = yield render_page(platform, consent_prompt)

                # If the participant wants to donate the data gets donated
                if consent_prompt_result.__type__ == "PayloadJSON":
                    yield donate(f"{session_id}-{platform}", consent_prompt_result.value)

                break

            # Sad flow
            # The data was not valid, ask the participant to retry
            if is_data_valid == False:
                retry_prompt = generate_retry_prompt(platform)
                retry_prompt_result = yield render_page(platform, retry_prompt)

                # The participant wants to retry: start from the beginning
                if retry_prompt_result.__type__ == 'PayloadTrue':
                    continue
                # The participant does not want to retry or pressed skip
                else:
                    break

        # The participant did not submit a file and pressed skip
        else:
            break

    yield exit_port(0, "Success")
    yield render_end_page()

```

**The functions used in `process`**

These are all the functions used in `process` together they make up `script.py` (Click on the functions to expand)

<details>
<summary>extract_the_data_you_are_interested_in</summary>

```python
def extract_the_data_you_are_interested_in(zip_file: str) -> pd.DataFrame:
    """
    This function extracts the data the researcher is interested in

    In this case we extract from the zipfile:
    * The filesnames
    * The compressed file size
    * The file size

    You could extract anything here
    """
    names = []
    out = pd.DataFrame()

    try:
        file = zipfile.ZipFile(zip_file)
        data = []
        for name in file.namelist():
            names.append(name)
            info = file.getinfo(name)
            data.append((name, info.compress_size, info.file_size))

        out = pd.DataFrame(data, columns=["File name", "Compressed file size", "File size"])

    except Exception as e:
        print(f"Something went wrong: {e}")

    return out
```

</details>

<details>
<summary>validate_the_participants_input</summary>

```python
def validate_the_participants_input(zip_file: str) -> bool:
    """
    Check if the participant actually submitted a zipfile
    Returns True if participant submitted a zipfile, otherwise False

    In reality you need to do a lot more validation.
    Some things you could check:
    - Check if the the file(s) are the correct format (json, html, binary, etc.)
    - If the files are in the correct language
    """

    try:
        with zipfile.ZipFile(zip_file) as zf:
            return True
    except zipfile.BadZipFile:
        return False
```

</details>

<details>
<summary>render_end_page</summary>

```python
def render_end_page():
    """
    Renders a thank you page
    """
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)

```

</details>

<details>
<summary>render_page</summary>

```python
def render_page(platform: str, body, progress: int):
    """
    Renders the UI components
    """
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform }))
    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)
```

</details>

<details>
<summary>generate_retry_prompt</summary>

```python
def generate_retry_prompt(platform: str) -> props.PropsUIPromptConfirm:
    """
    Generates a retry screen, this can be used if validation failed for example.
    You can aks the participant to try again, and go back to the submit file prompt
    """
    text = props.Translatable({
        "en": f"Unfortunately, we cannot process your {platform} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
        "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
    })
    ok = props.Translatable({
        "en": "Try again",
        "nl": "Probeer opnieuw"
    })
    cancel = props.Translatable({
        "en": "Continue",
        "nl": "Verder"
    })
    return props.PropsUIPromptConfirm(text, ok, cancel)
```

</details>


<details>
<summary>generate_file_prompt</summary>

```python
def generate_file_prompt(platform: str) -> props.PropsUIPromptFileInput:
    """
    Generates a prompt that asks the participant to input a file
    """
    description = props.Translatable({
        "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a {platform} file. ",
        "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
    })
 
    return props.PropsUIPromptFileInput(description, "application/zip, text/plain")
```

</details>

<details>
<summary>generate_consent_prompt</summary>

```python
def generate_consent_prompt(df: pd.DataFrame) -> props.PropsUIPromptConsentForm:
    """
    Generates a prompt screen with table(s) with the extract data for the participant to review
    """
    table_title = props.Translatable({
        "en": "Zip file contents",
        "nl": "Inhoud zip bestand"
    })
    table = props.PropsUIPromptConsentFormTable("zip_contents", table_title, df)
    return props.PropsUIPromptConsentForm([table], [])
```

</details>

<details>
<summary>donate</summary>

```python
def donate(key, json_string):
    """
    Sends data to the backend
    """
    return CommandSystemDonate(key, json_string)
```

</details>


<details>
<summary>exit_port</summary>

```python
def exit_port(code, info):
    """
    When in Next this function exits the data donation task, and lets the participant return to Next
    """
    return CommandSystemExit(code, info)
```

</details>


### Start writing your own `script.py` using the api

Now that you have seen a full example, you can start to try and create your own data donation task. With the elements from the example you can already build some pretty intricate data donation tasks.
Start creating your own by `script.py` by adapting this example to your own needs, for example, instead of file names you could extract data you would actually like to extract yourself.

If you want to see which up what UI elements are available to you checkout `api/props.py`. In general you need to construct your own pages (prompts) and render them with `render_page` (dont forget `yield`).

### The usage of `yield` in `script.py`

Did you notice `yield` instead of return? `yield` makes sure that whenever the code resumes after a page render, it starts where it left off.
If you render a page you need to use yield instead of return, just like in the example.

### Install Python packages

The data donation task runs in the browser of the participant, it is important to understand that when Python is running in your browser it is not using the Python version you have installed on your system.
The data donation task is using [Pyodide](https://pyodide.org/en/stable/) this is Python compiled to web assembly that runs in the browser. 
This means that packages you have available on your system install of Python, won't be available in the browser.

If you want to use external packages they should be available for Pyodide, you can check the list of available packages [here](https://pyodide.org/en/stable/usage/packages-in-pyodide.html).
If you have found a package you want to use you can installed it by adding it to the array in the `loadPackages` function in `src/framework/processing/py_worker.js` as shown below:

```javascript
// src/framework/processing/py_worker.js
function loadPackages() {
  console.log('[ProcessingWorker] loading packages')
  // These packages are now installed and usable: micropip, numpy, pandas, and lxml
  return self.pyodide.loadPackage(['micropip', 'numpy', 'pandas', 'lxml'])
}
```

You can now import the packages as you would normally do in Python.

### Try the donation task from the perspective of the participant

If you want to try out the above example, follow the installation instructions and start the server with `npm run start`.

### Tips when writing your own `script.py`

**Split the extraction logic from the data donation task logic**
You can define your own modules where you create your data extraction, you can `import` those modules in `script.py`

**Develop in separate script**
You are better off engineering your extraction logic in different scripts and put them in `script.py` whenever you are finished developing. Only do small tweaks in `script.py`

**Use the console in your browser**
In case of errors they will show up in the browser console. You can use `print` in the Python script and it will show up in the browser console.

**Keep the diverse nature of DDPs into account**
At least check a couple of DDPs to make sure its reflective of the population you are interesed in. Thinks you can check are: data formats (html, json, plain text, csv, etc.), language settings (they somethines lead to json keys being in a different language or file names other than English).

**Keep your code efficient**
If your code is not efficient the extraction will take longer, which can result in a bad experience for the participant. In practice I have found that in most cases it's not really an issue, and don't have to pay that much attention to efficiency of your code.
Where efficiency really matters is when you have parse huge html files, beautifulsoup4 is a library that is commonly used to do this, this library is too slow however. As an alternative you can use lxml which is fast enough.


**Don't let your code crash**
You cannot have your script crash, if your Python script crashes the task stops as well. This is not a good experience for your participant.
For example in the code you do the following: `value_i_want_to_extract = extracted_data_in_a_dictionary["interesting key"]` if the key `"interesting key"` does not exists, because it does not occur in the data of the participant, the script crashes and the participant cannot continue the data donation task.

**Data donation checklist**
Creating a good data donation task can be hard due to the variety of DDPs you will encounted. 
Check out the following [wiki article](https://github.com/d3i-infra/data-donation-task/wiki/Data-donation-checklist)


## Limits of the data donation task

Currently the data donation task has the following limitations:

* The data donation task is a frontend, you need to package this together with Next to deploy it. If you want to use it with your own backend you have to make the data donation task compatible with it yourself. A tutorial on how to do this might be added in the future.
* The data donation task is running in the browser of the participant that brings in limitations, such as constraints on the files participant can submit. The limits are around 2GiB thats what Pyodide can handle. But less is better. So keep that in mind whenever you, for example, want to collect data from YouTube: your participants should exclude their own personal videos from their DDP (including these would result in a huge number of separate DDPs of around 2GiB).
* The data donation currently works with text data, nothing keeps us from using other formats in the future (but the constraints on file sizes are still there)
* The data donation task should run fine on mobile, however its not optimized for it, you might need to do some tweaking to the UI yourself
