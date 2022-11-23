# Simple Server Maj

## Overview
This server has been build for a simple customer use case. The server downloads
two Json files from two soures, selects certain Json fields and appends the
results to a table. The data can be queried with pagination.

## Preparation
The  `server.py` file imports `headers.py` which declares two global variables:

```
verintHeaders = {
    'Accept': 'application/json',
    # and some secret keys
}

tsqHeaders = {
    'Accept': 'application/json',
    # and some secret keys
}
```

## API

### Trigger remote download
Use this method to download data from the servers. This can take several minutes

```
<server>/trigger
```

### Trigger local update
Use this method to load a local copy. This takes several seconds.

```
<server>/trigger?local=yes
```

### Get data

Get employee data with default paging (page=0 and page_size=5).
```
<server>/employees
```

Get employee data with dedicated paging parameters.
```
<server>/employees?page=0&page_size=10
```

Get employee data with one filter.
```
<server>/employees?page_size=10&page=0&first_name=Nkw
```

Get employee data with two filters.
```
<server>/employees?page_size=10&page=0&first_name=Nkw&last_name=Ngq
```
