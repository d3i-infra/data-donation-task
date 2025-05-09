# Installation

This guide covers the installation of the data donation task locally, so you can start creating your own data donation study!

## Installation Guide for the Pre-requisites

You need to install the following software:

- Python: Make sure it is at least version 3.10
- Node.js: Make sure it is at least version 16
- [Poetry](https://python-poetry.org/): It is a build system for Python packages that the data donation task uses. 

Below you can find more detailed instructions on how to install the required software depending on your operating system. 
These instructions are just suggestions, always prefer the official instructions that suite your situation best.

### Linux

You can install Python and Node.js from the official repositories of your distribution. Here are the general steps:

1. Open your terminal
2. Use your package manager to install Python and Node.js
3. Install Poetry using pipx, see the instruction [manual](https://python-poetry.org/docs/)

### Mac OSX 

If you are using a Mac OSX, you can install Python and Node.js using the [HomeBrew](https://brew.sh/) package manager. Follow these steps:

1. Open your terminal
2. Install HomeBrew following [instructions](https://brew.sh/) if you haven't already
3. Install Python and Node.js by runnning: `brew install python node`
4. Install Poetry using pipx, see the instruction [manual](https://python-poetry.org/docs/)


### Windows

In order to develop on Windows we recommend using Windows Subsystem for Linux (WSL) in combination with VSCode. 
Windows subsystem for Linux is a convenient way of running Linux on Windows.
This section will contain a bit more context because the steps might be less familiar to Windows only users.

If you are already familiar with WSL/Linux, VSCode or both, the installation won't give you too much trouble.
If you are completely new to WSL (or Linux) expect a certain amount of problem solving you have to do.
Key topics to understand are: WSL, and the Ubuntu basics; knowledge on these topics will help you a lot. 

1. Install WSL, see the official installation [instructions](https://learn.microsoft.com/en-us/windows/wsl/install)
2. Install the default Linux distro (Ubuntu 22.04 at the time of writing) and choose a username and password
3. Download and install VSCode
4. Connect VSCode to WSL, see [instructions](https://code.visualstudio.com/docs/remote/wsl-tutorial)
5. Now you can follow the instructions for Linux, Note that Python will be already installed for you

In theory these steps should cause no problems but in reality you have a couple of issues you could run into. I will discuss some of them I encountered here: 

* You have the be an administrator of your own device. If you are not an administrator you cannot continue the installation 
* In order to install WSL, kernel virtualization needs to be on. You can go into the Windows Task Manager and check whether it is on. If its not on, you have to turn it on in the bios of your motherboard. Check what CPU you have (AMD or Intel) and check what the settings is called. If the setting is not present in the bios your CPU might not support virtualization, this means you cannot run WSL
* If you have WSL 1 installed make sure you continue with WSL 2
* Make sure you don't forget the username and password you chose during the installation
* If you have VSCode open make sure you are connected to WSL, you can check this by looking at the "><" icon in the lower left corner of VSCode
* Remember that if you are connected to WSL with VSCode you are working in Ubuntu. Programs and files are not shared between Windows and Ubuntu, meaning if you have installed a program on Windows is not available for Ubuntu and vice versa.
* Remember to not use Powershell when connected to WSL use bash
* If you see error messages related to Windows in the terminal (something with cmd.exe for example), you know that Ubuntu is trying to open a program on Windows. This will never work. This is happening because Windows manipulates the PATH variable on Ubuntu that contains information about where the programs Ubuntu can open are. Example: you want to check which version of node you have `node -v` and you get an error with cmd.exe in the error message. Solutions: uninstall the windows version of the Node.js or manipulate the PATH variable so it does not try to open the Windows version of Nodejs. How to do that is outside the scope of this manual.
* To run port you need version Nodejs version 18 this version is not in the official Ubuntu 22.04 repositories. See for example this [guide](https://www.digitalocean.com/community/tutorials/how-to-install-node-js-on-ubuntu-22-04) on how to get nodejs version 18. If you run into errors you are expected to search for them and to solve them

#### Don't want to use WSL?

That's completely fine too, you can change the commands in `package.json` so they work on Windows instead.

## Installation of the data donation task

If you have the Pre-requisites installed the installation of the data donation task should be straightforward.

1. Clone the repository:

```
git clone https://github.com/d3i-infra/data-donation-task.git
```

2. Install the dependencies by running the following commands:

```
cd ./data-donation-task
npm install
```

3. Start a local web server to server the data donation app: 

```
npm run start
```

You can now go to the browser: [`http://localhost:3000`](http://localhost:3000) and you should be greeted by a mock data donation task
