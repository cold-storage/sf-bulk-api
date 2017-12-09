# sf-bulk-api

For Node.js - implements most of the original Salesforce Bulk API

**Install**

```sh
npm i sf-bulk-api
```

**Enjoy**

```js
const BulkApi = require('sf-bulk-api');
const options = require('../opts');
const bulkApi = new BulkApi(options);
const csvAppendStream = require('csv-append-stream');

bulkApi
  .addBatch('select Id from Account limit 100')
  .then(somehowPollTillJobSuccess)
  .then(bulkApi.getQueryResults)
  .then((stream) => {
    csvAppendStream(stream).pipe(process.stdout);
  })
  .catch((err) => {
    console.error(err);
  });
```

The only missing piece above is `somehowPollTillJobSuccess`. Leave that as an
exercise to the reader. Should be simple. Keep calling `getJobInfo()` till it's
all success.
