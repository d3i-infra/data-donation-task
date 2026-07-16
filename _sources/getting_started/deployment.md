# Deployment

The data donation task is deployed via [Next](https://datadonation.eu/software/getting-started/), a SaaS platform developed by [Eyra](https://eyra.co/). Next allows you to configure a complete data donation study, including participant landing pages, task lists, and data storage.

## Getting started with Next

Visit [datadonation.eu/software/getting-started/](https://datadonation.eu/software/getting-started/) for full instructions on how to set up and configure a study on Next.

You can use the software for free with support from the D3I project. To get started, get in touch with [Laura Boeschoten](mailto:l.boeschoten@uu.nl).

## Add your data donation task to Next

After building your data donation task, create a release zip and upload it to Next.

### Building a release

Each platform needs a config file before it can be built. If you have not done this yet, generate one first:

```sh
pnpm generate-config <platform>
# example: pnpm generate-config instagram
```

To build a zip for every platform at once:

```sh
pnpm release
```

This creates one zip per platform in the `releases/` folder. The list of platforms is determined automatically by which config files exist in `packages/python/port/configs/`.

To build a zip for just one platform:

```sh
VITE_PLATFORM=instagram pnpm release
```

### Uploading to Next

1. Run the release command above to create the zip file(s).
2. In Next, go to your study's workflow and create a new task list item called "data donation task".
3. Select the zip file for the platform you want to deploy.

Repeat step 2–3 for each platform in your study.
