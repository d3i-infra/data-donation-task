# Data donation checklist

Writing data donation scripts can be challenging due to the diverse nature of data download packages (DDPs) your participants will try to submit. 
If your participants try to submit a DDP that you did not anticipate your extraction might fail, or your script might crash, in that case you will end up with a non-response and a frustrated participant.

In order to minimize the number of problems you encounter. We developed a list of points you can pay attention to when developing scripts or performing your own study.


**Inspect at least 5 DDPs from persons in the population you are going to sample from**

Because DDPs will be different for different people, you need to inspect a couple of them (preferably from people from your target population) to learn what those differences are.
You need to verify that the extraction you have in mind works for all DDPs you might encounter.


**DDP formats will change over time**

DDP formats will change over time. Organization providing you with a DDP are under no obligation to keep the format of their DDP the same. The formats could change during data collection, be mindful of this fact. 
Before going live with a study request a fresh package of your own and check whether your extraction still works as expected, and in the worst case scenario be ready to change your script during your data donation study.


**Validate the DDPs your participant submit and log the results**

This is crucial to do for two reasons: 

1. If there are multiple versions of a DDP, you need to know which version the participant submitted so you can handle it appropriately. As an example consider the Youtube DDP: depending on the language setting files in the DDP are named differently. Another example is for the Instagram DDPs, keys in json files can be different depending on the language. 
2. You probably want to give participants an incentive whenever they did a serious attempt of donating their data. In order to know whether they did a serious attempt you need to validate their DDP to see whether they submitted a package you expect. Example: a participant wants to participate in your study, but finds out its way too much work to request and download a DDP. They submit a random zipfile containing garbage, and they claim they completed the process succesfully. The only way for you to verify whether the participant gave it a fair shot is, to check what they submitted and why that did not result in you receiving data from that participant (If you would have received data its easy to verify they participated).


**Write your extraction so it matches the DDP request defaults**

Often when requesting a DDP participants have a choice, for example you can request a portion of the DDP in html format or json format. The most human readible format (html) is often the default. It will be worth while to tailor your extraction script to the request defaults because participants will not read your instructions where you tell them to get the DDP in json format instead of html. This goes wrong quite often.

Our way of dealing with this is to bite the bullet and deal with the default DDP formats. This prevents mistakes and keeps the requests instruction for the participant as simple as possible.
Another way of dealing with it is to provide feedback to the participant whenever you detected they submitted the DDP in a format you did not anticipate.


**Make sure your code will not crash**

A crash in your code causes the data donation task to be stuck. The participant will see an error screen with the stacktrace displayed. You don't want this to happen. Carefull programming can prevent your code from crashing.
A common cause for crashes is trying to access a key value pair in a dict that does not exist or sorting a `pd.DataFrame` column that does not exist. Most crashes will be caused by your expectation that the extraction is in a certain format, while in some cases it won't be.


**Make the least amount of assumptions possible about the data in a DDP**

The more assumptions you make about the data the more opportunities your code has to fail. Some examples:

* Date parsing: Only parse date when its absolutely required. Date formats can be wildly different between participants, anticipating them all or writing code that can parse all dates you might encounter is less trivial than you might think.
* Files in a zip: You can look for file paths you are interested in, or only file names you are interested in. If the file name is unique, there is no need to check for the full file path. Example: if the folder structure in a zip changes but files remain the same, the code that looks only at file names will still work.
* Nested dictionaries: Sometimes you are interested in a value in a dict that is heavily nested. An approach you can take, instead of doing `dict["key1"]["key2"]["key3"]["key_that_sometimes_is_called_something_different"]["key_which_value_you_want"]`, you can to first denest or flatten the dictionary start looking directly for "key_which_value_you_want". You can find an example [here](https://github.com/d3i-infra/port-vu-pilot/blob/master/src/framework/processing/py/port/helpers.py), look for `dict_denester` and `find_items`. 


**The researcher is responsible for providing you with DDPs and should be knowledgeable about the data**

If you are reading this checklist chances are you are going to create a data donation task. It could be the case that you are not the principal investigator of the project but just an engineer or person with some coding experience helping the researcher out. Some researchers expect you to be the one to have knowledge about a certain DDP they are interested in. Some researchers believe that because you are working with data, you also have that data available to you, know what it all means or whether data is present in a DDP. This is of course not always the case. Communicate clearly to the researcher that they responsible for the contents of their study, they should know what the data means and that they should tell you what to extract. In some cases the researcher might not even use the platform they are interested in, if that is the case, tell the researcher to start using the platform they are interested in so they can start instructing you on what to extract.


**Test a study carefully before you go live**

All researchers involved in the research group should test the study before you go live. A data donation study has more points of failure compared to traditional survey research, therefore its crucial that every researcher that is involved will test the complete data donation flow and shares their findings with you.
