# Creating your own data donation task


After you have forked or cloned and installed the repository you can start creating your own donation task. 

You can create your own study by changing and/or adapting the code in the following directory `packages/python/port`
This directory contains the following files:

* `d3i_example_script.py`: Contains your donation task logic; which screen the participants will see and in what order
* `api/commands.py`: Contains the render and the donate commands

## `d3i_example_script.py`

Lets look at the example script in [`d3i_example_script.py`](https://github.com/d3i-infra/data-donation-task/blob/master/packages/python/port/d3i_example_script.py). 

`d3i_example_script.py` must contain a function called `process` this function determines the whole data donation task from start to finish (Which screens the participant will see and in what order, and what kind of data extraction will take place). 

In this example process defines the following data donation task:

1. Ask the participant to submit a zip file
2. Perform validation on the submitted zip file, if not valid return to step 1
3. Extract the data from the submitted zip file
4. Render the extract data on screen in a table
5. Send the data to the data storage upon consent

Although these can vary per data donation task, they will be mostly similar.

The `d3i_example_script.py` is annotated, read it carefully and see whether you can follow the logic.


## Start writing your own `d3i_example_script.py` using the api

Now that you have seen a full example, you can start to try and create your own data donation task. With the elements from the example you can already build some pretty intricate data donation tasks.
Start creating your own by `d3i_example_script.py` by adapting this example to your own needs, for example, instead of file names you could extract data you would actually like to extract yourself.

If you want to see which up what UI elements are available to you checkout `api/props.py` or `api/d3i_props.py`.

## The usage of `yield` in `d3i_example_script.py`

`yield` makes sure that whenever the code resumes after a page render, it starts where it left off.
If you render a page you need to use yield instead of return, just like in the example.

## Install Python packages

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

## Try the donation task from the perspective of the participant

If you want to try out the above example, follow the installation instructions and start the server with `pnpm run start`.

## Tips when writing your own `d3i_example_script.py`

**Split the extraction logic from the data donation task logic**
You can define your own modules where you create your data extraction, you can `import` those modules in `d3i_example_script.py`

**Develop in separate script**
You are better off engineering your extraction logic in different scripts and put them in `d3i_example_script.py` whenever you are finished developing. Only do small tweaks in `d3i_example_script.py`

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
* The data donation task is running in the browser of the participant that brings in limitations, such as constraints on the files participant can submit. The limits are around 4 GiB thats what Pyodide can handle. But less is better. So keep that in mind whenever you, for example, want to collect data from YouTube: your participants should exclude their own personal videos from their DDP (including these would result in a huge number of separate DDPs of around 4 GiB).
