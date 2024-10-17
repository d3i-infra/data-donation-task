# Deployment of the data donation task

This article will discuss the option you have when using the data donation task in a data donation study. 
The data donation task is designed to be used with Next.

## The data donation task with Next

Next is a software as a service platform developed by [Eyra](https://eyra.co/).
As a researcher you can log in to Next and configure data donation study, this means:

1. Configuring a landing zone for your participants: I.e. a Consent form page, an information page, a privacy policy
2. Configure a task list for your participants to complete: After the landing zone participants see a list of task they have to complete, typically these tasks are: viewing instruction on how to request and download data from a specific platform and administering the data donation task that you developed using this repository
3. Configuring where the donated data should be stored. Next has solutions for: AWS, Azure and Yoda.

After configuration participants can be sent to Next with a unique id in the url. This unique key id be used when storing the data, so you know who donated their data


### Next as a paid service

You can use Next as a paid service provided by [Eyra](https://eyra.co/). 
Please contact Eyra if this is something you are interested in.


### Self service Next (community version) on Surf Research Cloud

Next is available as an offering on Surf Research Cloud available for Researchers at Dutch universities and universities of applied sciences.
Dutch researchers can apply for an EINFRA grant and get access to Research cloud this way. You can apply for an EINFRA grant [here](https://www.surf.nl/en/small-compute-applications-nwo) and click "Straight to the request portal".

This offering comes with no service or warranties. Contact [datadonation.eu](https://datadonation.eu/) if you are interested in setting this up.


### Self service Next (community version)

Next is a free and open source tool and you could host it yourself. You can find Next [here](https://github.com/eyra/mono/blob/master/SELFHOSTING.md)


### Which option should I choose?

* Next as a paid service: If you have research budget; want to be unburdened and get your data donation study done, this is the best option.
* Self service community Next on Surf Research Cloud: You are a researcher at a Dutch university with no budget this is the best option. When choosing this option you have to realize that it comes with no service or warranties, you have to know what you are doing.
* Self service community Next: If you want to provide Next as a service to your organization.


### Add data donation task to your data donation study on Next

After you have created your data donation task with this repository, you can use this task directly in Next. You can do this as follows:

1. In the data donation task run the command `npm run release`, this creates a zip file named `release.zip`
2. In Next when configuring your data donation study, go to work flow and create a new item task list item called data donation task
3. In the newly created task list item select the `release.zip` you have just created

Your data donation task list item is configured!


## Use the data donation task without Next

The data donation task can be adapted so it works with your own bespoke back end. A tutorial on how to do this is might be added in the future.
