# The data donation task

The data donation task is front end that guides participants through the data donation steps, used in conjunction with Next.
Next is a software as a service platform developed by [Eyra](https://eyra.co/) to facilitate scientific research.

## Documentation

Here you can find the [documentation](https://d3i-infra.github.io/data-donation-task/) of this repository and tutorial articles to get you going.


## Installation of the data donation task

In order to start a local instance of the data donation task go through the following steps:

0. Pre-requisites

   - Fork or clone this repo
   - Install [Node.js](https://nodejs.org/en)
   - Install [Python](https://www.python.org/)
   - Install [Poetry](https://python-poetry.org/)

1. Install dependencies & tools:

   ```sh
   npm install
   ```

2. Start the local web server:

   ```sh
   npm run start
   ```

3. You can now go to the browser: [`http://localhost:3000`](http://localhost:3000).

If the installation went correctly you should be greeted with a mock data donation study. 
For detailed installation instructions see the [documentation](https://d3i-infra.github.io/data-donation-task/).


## Contributing

We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features

If you have any questions, find any bugs, or have any ideas, read how to contribute [here](https://github.com/eyra/port/blob/master/CONTRIBUTING.md).


## Citation

If you use this repository in your research, please cite it as follows:

```
@article{Boeschoten2023,
  doi = {10.21105/joss.05596},
  url = {https://doi.org/10.21105/joss.05596},
  year = {2023},
  publisher = {The Open Journal},
  volume = {8},
  number = {90},
  pages = {5596},
  author = {Laura Boeschoten and Niek C. de Schipper and Adriënne M. Mendrik and Emiel van der Veen and Bella Struminskaya and Heleen Janssen and Theo Araujo},
  title = {Port: A software tool for digital data donation},
  journal = {Journal of Open Source Software}
}
```

You can find the full citation details in the [`CITATION.cff`](CITATION.cff) file.
