# tap-pinterest-ads

`tap-pinterest-ads` is a Singer tap for Pinterest Ads.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

**Note:** Ad Analytics are filtered for `ACTIVE` ads only, hence totals may not match that of the account analytics, this is done to reduce the number of requests due to the limits imposed by the API.

## Installation

```bash
pipx install git+https://github.com/gthesheep/tap-pinterest-ads.git
```

## Configuration

### Accepted Config Options

- **client_id**: App ID for your Pinterest App
- **client_secret**: App secret key
- **refresh_token**: Refresh token obtained from the OAuth user flow
- **start_date**: Start date to collect ad analytics from

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-pinterest-ads --about
```

### Source Authentication and Authorization

In order to obtain the ```refresh_token``` for your Pinterest Ads account
please follow the OAuth 2.0 flow described by the source docs, [here](https://developers.pinterest.com/docs/api/v5/#tag/Authentication).
We can look to add support for this process in here in the future.

Beyond obtaining the Trial Access for the API, filling of historical data
may require upgrading to Standard Access, depending on use of the ads service.

## Usage

You can easily run `tap-pinterest-ads` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-pinterest-ads --version
tap-pinterest-ads --help
tap-pinterest-ads --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_pinterest_ads/tests` sub-folder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-pinterest-ads` CLI interface directly using `poetry run`:

```bash
poetry run tap-pinterest-ads --help
```
