# sf-bulk-api

For Node.js - implements most of the original Salesforce Bulk API

**STATUS**

Unstable. Not tested. We will stay on version 0.0.x till things are at least
beta quality.

**Install**

```sh
npm i sf-bulk-api
```

**Enjoy**

```js
const BulkApi = require('sf-bulk-api');
const bulkApi = new BulkApi(options);

bulkApi
  .addBatch('select Id from Account limit 100')
  .then(waitForJobToComplete)
  .then(bulkApi.getQueryResults.bind(bulkApi))
  .then((stream) => {
    stream.pipe(process.stdout);
  })
  .catch((err) => {
    console.error(err);
  });
```

The only missing piece above is `waitForJobToComplete`.  Should be simple. Keep
calling `getJobInfo()` till it's all success.
